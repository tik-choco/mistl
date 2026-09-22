import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const html=fs.readFileSync(new URL('../src/web/assets/index.html',import.meta.url),'utf8');
const source=html.slice(html.indexOf('  function pageIdentity()'),html.indexOf('  function withBusy('));
const banner={hidden:true};
const button={disabled:true,setAttribute(k,v){this[k]=v;},addEventListener(k,v){this[k]=v;}};
const calls=[];
const build={channel:'dev',version:'1.2.3',instance:'repro',instance_id:'mistl-dev-repro',build_id:'0123456789abcdef',mistlib_version:'0.6.2',profile:'release'};
const context=vm.createContext({state:undefined,currentLang:'ja',window:{mistlRuntime:build},
  document:{getElementById:id=>id==='build-banner'?banner:button},
  api:(cmd,args)=>{calls.push({cmd,args});return Promise.resolve({enabled:args.enabled,state:'restarting',saved:true});},
  showToast:()=>assert.fail('unexpected toast')});
vm.runInContext(source,context);
assert.equal(banner.hidden,false,'dev metadata must render before the first daemon poll');
assert.match(banner.textContent,/DEV · mistl v1\.2\.3 · 0123456789ab/);
assert.equal(button.disabled,true);
assert.equal(context.pageIdentity(),'mistl-dev-repro/0123456789abcdef');
context.state={daemon:{build,network:{enabled:false,state:'off',saved:true}}};
context.renderRuntime();
assert.equal(button.textContent,'外部接続: OFF'); assert.equal(button['aria-pressed'],'false');
assert.equal(button.disabled,false);
button.click.call(button); await new Promise(r=>setImmediate(r));
assert.equal(calls[0].cmd,'network.set'); assert.equal(calls[0].args.enabled,true);
assert.equal(button.disabled,true); assert.match(button.textContent,/切替中/);
context.state.daemon.network={enabled:false,state:'off',saved:false,error:'disk full'};
context.renderRuntime(); assert.match(button.textContent,/未保存/); assert.equal(button.title,'disk full');
context.currentLang='en'; context.state.daemon.network={enabled:true,state:'on',saved:true};
context.renderRuntime(); assert.equal(button.textContent,'External connections: ON');
context.state.daemon.build={...build,channel:'stable'}; context.renderRuntime();
assert.equal(banner.hidden,true,'stable does not retain a development banner');
assert.equal((html.match(/setRequestHeader\("x-mistl-instance", pageIdentity\(\)\)/g)||[]).length,3,'all upload paths must identify their source page');
console.log('PASS: pre-poll development banner, optimized dev identity, ON/OFF accessibility, shared toggle API, pending state, save errors, language change, stable banner, upload guards');
