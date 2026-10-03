// Check the actual dashboard catalogs without booting the application.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const start = html.indexOf('  var T = {');
const end = html.indexOf('  function detectBrowserLang()', start);
assert.ok(start >= 0 && end > start, 'dashboard catalog boundaries exist');
const catalogs = vm.runInNewContext(html.slice(start, end) + '\nT;', {}, {timeout:1000});
const placeholders = value => [...value.matchAll(/\{([^{}]+)\}/g)].map(match => match[1]).sort();

function checkCatalogs(catalogs) {
  const problems = [];
  const reference = catalogs.en;
  assert.ok(reference && Object.keys(reference).length, 'English reference catalog exists');
  for (const locale of ['en', 'zh', 'ja']) {
    const catalog = catalogs[locale];
    if (!catalog) { problems.push(locale + ': missing catalog'); continue; }
    for (const key of Object.keys(reference)) {
      const value = catalog[key];
      if (!Object.hasOwn(catalog, key) || typeof value !== 'string' || !value.trim()) {
        problems.push(locale + ': missing or empty ' + key);
      } else if (JSON.stringify(placeholders(value)) !== JSON.stringify(placeholders(reference[key]))) {
        problems.push(locale + ': placeholder mismatch ' + key);
      }
    }
    for (const key of Object.keys(catalog)) {
      if (!Object.hasOwn(reference, key)) problems.push(locale + ': orphan ' + key);
    }
  }
  return problems;
}

// Ensure regressions are rejected, including repeated and reordered placeholders.
const sample = {en:{label:'{model} in {room} ({model})'},zh:{label:'{room}: {model} ({model})'},ja:{label:'{model} ({model}) - {room}'}};
assert.deepEqual(checkCatalogs(sample), []);
for (const change of [
  catalog => { delete catalog.label; },
  catalog => { catalog.label = '   '; },
  catalog => { catalog.orphan = 'extra'; },
  catalog => { catalog.label = '{model} in {room}'; },
  catalog => { catalog.label = '{model} in {provider} ({model})'; }
]) {
  const altered = structuredClone(sample);
  change(altered.zh);
  assert.ok(checkCatalogs(altered).length, 'catalog regression is detected');
}
assert.deepEqual(checkCatalogs(catalogs), [], 'dashboard language coverage');
console.log('PASS: dashboard i18n; en/zh/ja, ' + Object.keys(catalogs.en).length + ' non-empty keys each, no orphans, matching placeholders');
