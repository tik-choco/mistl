import { BAND_HEIGHT_FRAC, BANDS, decodeBands, drawBands, expandClock } from '/pattern.mjs';

const params = new URLSearchParams(location.search);
const room = params.get('room');
const nodeId = params.get('node');
const capture = params.get('capture') ?? 'tab';
const audio = params.get('audio') ?? '1';
const encoding = params.get('encoding') ?? '1';
const hint = params.get('hint') ?? '';
const degradation = params.get('degradation') ?? '';
const canvas = document.querySelector('canvas');
const pcs = new Set();
let node;
let engine;
let ready;
let width, height, fps, maxBitrate, maxFps;

const lab = window.__lab = {
  state: 'init',
  error: null,
  nodeId,
  room,
  startCapture,
  framesDrawn: 0,
  publishedAt: null,
  encodingApplied: null,
  stats: [],
  log: [],
  selfCheck: null,
};

function appendCapped(array, value, limit = 600) {
  array.push(value);
  if (array.length > limit) array.shift();
}

for (const level of ['warn', 'error']) {
  const original = console[level].bind(console);
  console[level] = (...args) => {
    appendCapped(lab.log, `${level}: ${args.map(String).join(' ')}`);
    original(...args);
  };
}
window.addEventListener('error', event => appendCapped(lab.log, event.message));
window.addEventListener('unhandledrejection', event => appendCapped(lab.log, String(event.reason)));

window.RTCPeerConnection = class extends window.RTCPeerConnection {
  constructor(...args) {
    super(...args);
    pcs.add(this);
  }
};

const outboundFields = `frameWidth frameHeight framesPerSecond framesSent bytesSent
  targetBitrate qualityLimitationReason qualityLimitationDurations nackCount pliCount firCount
  retransmittedBytesSent encoderImplementation`.split(/\s+/);
const remoteInboundFields = ['packetsLost', 'roundTripTime', 'fractionLost'];
const candidatePairFields = ['availableOutgoingBitrate', 'currentRoundTripTime'];

function pick(report, fields) {
  return Object.fromEntries(fields.filter(field => field in report).map(field => [field, report[field]]));
}

async function samplePeer(pc) {
  const { connectionState, signalingState } = pc;
  const outbound = [];
  const remoteInbound = [];
  let candidatePair = null;
  try {
    const reports = await pc.getStats();
    for (const report of reports.values()) {
      if (report.type === 'outbound-rtp' && report.kind === 'video') {
        outbound.push(pick(report, outboundFields));
      }
      if (report.type === 'remote-inbound-rtp' &&
          (report.kind === 'video' || reports.get(report.localId)?.kind === 'video')) {
        remoteInbound.push(pick(report, remoteInboundFields));
      }
      if (report.type === 'transport' && report.selectedCandidatePairId) {
        const selected = reports.get(report.selectedCandidatePairId);
        if (selected) candidatePair = pick(selected, candidatePairFields);
      }
    }
  } catch (error) {
    console.warn('getStats failed:', error);
  }
  if (connectionState === 'closed') pcs.delete(pc);
  return { connectionState, signalingState, outbound, remoteInbound, candidatePair };
}

async function sampleStats() {
  const t = Date.now();
  appendCapped(lab.stats, { t, pcs: await Promise.all(Array.from(pcs, samplePeer)) });
  setTimeout(sampleStats, 1000);
}

function numberParam(name, fallback, min, max, integer = true) {
  const value = Number(params.get(name) ?? fallback);
  if (!Number.isFinite(value) || value < min || value > max || (integer && !Number.isInteger(value))) {
    throw new Error(`Invalid ${name}: ${params.get(name)}`);
  }
  return value;
}

function startDrawing() {
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext('2d', { alpha: false });
  const top = Math.ceil(height * BANDS * BAND_HEIGHT_FRAC);
  const textSize = Math.max(10, Math.round(height / 60));
  ctx.font = `${textSize}px monospace`;
  const interval = 1000 / fps;
  let next = performance.now();

  function draw() {
    const clock = Date.now();
    const counter = ++lab.framesDrawn;
    const shift = (counter * 3) % width;
    const gradient = ctx.createLinearGradient(shift - width, 0, shift + width, 0);
    for (const stop of [0, 0.5, 1]) gradient.addColorStop(stop, '#12324a');
    for (const stop of [0.25, 0.75]) gradient.addColorStop(stop, '#27666b');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, top, width, height - top);

    ctx.fillStyle = '#f2ba55';
    ctx.fillRect((counter * 7) % (width * 0.85), top + (height - top) * 0.35, width * 0.15, height * 0.12);
    ctx.fillStyle = '#fff';
    for (let y = top + textSize * 2; y < height; y += textSize * 3) {
      ctx.fillText(`Stream Lab  /  frame ${counter}  /  ${clock}  /  line ${y}`, textSize, y);
    }
    drawBands(ctx, width, height, counter, clock);

    const now = performance.now();
    next = next + interval > now ? next + interval : now + interval;
    setTimeout(draw, next - now);
  }
  draw();
}

async function join() {
  if (!room || !nodeId) throw new Error('Both room and node query parameters are required');
  if (!['tab', 'canvas'].includes(capture)) throw new Error(`Invalid capture mode: ${capture}`);
  width = numberParam('w', 1920, 32, 16384);
  height = numberParam('h', 1080, 12, 16384);
  fps = numberParam('fps', 30, 1, 1000, false);
  maxBitrate = numberParam('maxBitrate', 8000000, 0, 0xffffffff);
  maxFps = numberParam('maxFps', 30, 1, 1000, false);
  startDrawing();
  setTimeout(sampleStats, 1000);

  lab.state = 'joining';
  const [{ MistNode }, rawEngine] = await Promise.all([
    import('@tik-choco/mistlib-web'), import('@tik-choco/mistlib'),
  ]);
  engine = rawEngine;
  node = new MistNode(nodeId, {
    signaling: {
      mode: 'nostr',
      nostr: {
        relays: [],
        discoveryKind: 25049,
        messageKind: 25050,
        ttlSeconds: 600,
        inviteSalt: 'tik-choco-v1',
        inviteCode: 'tik-choco-public-v1',
      },
    },
  });
  await node.init();
  await node.joinRoomAsync(room);
  if (lab.state === 'joining') lab.state = 'joined';
}

function fail(error) {
  lab.state = 'error';
  lab.error ??= String(error);
  console.error(error);
}

async function captureTab() {
  let timeout;
  let expired = false;
  const requested = navigator.mediaDevices.getDisplayMedia({
    preferCurrentTab: true,
    video: {
      displaySurface: 'browser',
      width: { max: width },
      height: { max: height },
      frameRate: { ideal: fps, max: fps },
    },
    audio: false,
  }).then(stream => {
    if (expired) for (const track of stream.getTracks()) track.stop();
    return stream;
  });
  try {
    return await Promise.race([requested, new Promise((_, reject) => {
      timeout = setTimeout(() => {
        expired = true;
        reject(new Error('getDisplayMedia timed out'));
      }, 10000);
    })]);
  } finally {
    clearTimeout(timeout);
  }
}

async function checkCapture(track) {
  const video = document.createElement('video');
  video.muted = true;
  video.srcObject = new MediaStream([track]);
  const frame = document.createElement('canvas');
  frame.width = 320;
  frame.height = 180;
  const ctx = frame.getContext('2d', { willReadFrequently: true });
  let timeout, callback;
  try {
    return await new Promise((resolve, reject) => {
      timeout = setTimeout(() => reject(new Error('Could not decode the captured test pattern')), 5000);
      function checkFrame() {
        try {
          ctx.drawImage(video, 0, 0, frame.width, frame.height);
          const { data } = ctx.getImageData(0, 0, frame.width, frame.height);
          const decoded = decodeBands((x, y) => data[(y * frame.width + x) * 4], frame.width, frame.height);
          const now = Date.now();
          if (decoded && Math.abs(now - expandClock(decoded.clockLow32, now)) <= 5000) resolve(decoded);
          else callback = video.requestVideoFrameCallback(checkFrame);
        } catch (error) {
          reject(error);
        }
      }
      callback = video.requestVideoFrameCallback(checkFrame);
      video.play().catch(reject);
    });
  } finally {
    clearTimeout(timeout);
    video.cancelVideoFrameCallback(callback);
    video.pause();
    video.srcObject = null;
  }
}

async function startCapture() {
  if (!['joining', 'joined'].includes(lab.state)) throw new Error(lab.error ?? `Cannot start capture in state ${lab.state}`);
  lab.state = 'capturing';
  let stream, track, tone;
  const registered = [];
  try {
    try {
      // Call getDisplayMedia before awaiting anything to keep the caller's user activation.
      stream = capture === 'tab' ? await captureTab() : canvas.captureStream(fps);
      [track] = stream.getVideoTracks();
      if (!track || (capture === 'tab' && track.getSettings().displaySurface !== 'browser')) {
        throw new Error('No video track for the requested capture surface');
      }
      lab.selfCheck = await checkCapture(track);
    } catch (error) {
      if (capture === 'tab') throw new Error(`capture=tab failed (${error}). Use capture=canvas as a fallback.`);
      throw error;
    }

    await ready;
    if (lab.error) throw new Error(lab.error);
    if (hint) track.contentHint = hint;

    if (audio === '1') {
      tone = new AudioContext();
      const destination = tone.createMediaStreamDestination();
      const oscillator = tone.createOscillator();
      const gain = tone.createGain();
      gain.gain.value = 0.1;
      oscillator.connect(gain).connect(destination);
      oscillator.start();
      await tone.resume();
      stream.addTrack(destination.stream.getAudioTracks()[0]);
    }

    for (const media of stream.getTracks()) {
      const prefix = media.kind === 'video' ? 'tc-chat-screen' : 'tc-chat-screen-audio';
      const id = `${prefix}:${media.id}`;
      registered.push(id);
      if (media === track) lab.publishedAt = Date.now();
      node.registerLocalTrack(id, media, { publish: true, enabled: true });
      if (media === track && encoding === '1' && maxBitrate !== 0) {
        lab.encodingApplied = false;
        if (typeof engine.set_local_track_encoding === 'function') {
          try {
            engine.set_local_track_encoding(id, maxBitrate, maxFps, degradation || undefined);
            lab.encodingApplied = true;
          } catch (error) {
            console.warn('set_local_track_encoding failed:', error);
          }
        }
      }
    }
    lab.state = 'publishing';
  } catch (error) {
    fail(error);
    for (const id of registered) node.removeLocalTrack(id);
    for (const track of stream?.getTracks() ?? []) track.stop();
    if (tone) await tone.close();
    throw error;
  }
}

ready = join().catch(fail);
