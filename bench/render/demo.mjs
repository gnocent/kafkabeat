// Captures d'un dashboard Kibana à intervalle régulier, pour la démo animée du
// README (bench/demo.sh les assemble en GIF).
// Usage : node demo.mjs <kibana> <id du dashboard> <répertoire> <durée s> <pas s> [requête KQL]
import { chromium } from 'playwright-core';
import { mkdirSync } from 'node:fs';

const [kibana, id, dir, seconds, step, kql] = process.argv.slice(2);
if (!kibana || !id || !dir || !seconds || !step) {
  console.error('usage: node demo.mjs <kibana> <dashboard> <directory> <duration s> <step s> [KQL query]');
  process.exit(2);
}
const rison = (s) => s.replace(/[!']/g, '!$&');
const query = kql ? `&_a=(query:(language:kuery,query:'${encodeURIComponent(rison(kql))}'))` : '';
// rafraîchissement automatique toutes les 10 s, comme l'écran de supervision
const url = `${kibana}/app/dashboards#/view/${id}?_g=(time:(from:now-15m,to:now),refreshInterval:(pause:!f,value:10000))${query}`;

mkdirSync(dir, { recursive: true });
const browser = await chromium.launch({ args: process.env.KB_NO_SANDBOX ? ['--no-sandbox'] : [] });
try {
  // fenêtre haute : Kibana ne rend que les panneaux visibles, il faut tout le dashboard à l'écran
  const page = await browser.newPage({ viewport: { width: 1440, height: 3000 } });
  await page.goto(url, { timeout: 120000 });
  await page.waitForSelector('[data-shared-item][data-render-complete="true"]', { timeout: 120000 });
  await page.waitForTimeout(5000);
  // hauteur utile : bas du dernier panneau, sans le vide sous le dashboard
  const height = await page.evaluate(() => Math.ceil(Math.max(
    ...[...document.querySelectorAll('[data-shared-item]')].map((e) => e.getBoundingClientRect().bottom + window.scrollY)) + 16));
  const frames = Math.round(Number(seconds) / Number(step));
  for (let i = 0; i < frames; i++) {
    const t0 = Date.now();
    await page.screenshot({ path: `${dir}/frame-${String(i).padStart(4, '0')}.png`, fullPage: true, clip: { x: 0, y: 0, width: 1440, height } });
    await page.waitForTimeout(Math.max(0, Number(step) * 1000 - (Date.now() - t0)));
  }
} finally {
  await browser.close();
}
