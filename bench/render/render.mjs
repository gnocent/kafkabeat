// Rendu d'un dashboard Kibana dans Chromium headless (critères 8 et 9).
// Usage : node render.mjs <kibana> <id du dashboard> [from] [to] [capture.png] [requête KQL]
// Charge le dashboard une première fois (bundles mis en cache), puis le
// recharge et mesure le rendu de tous ses panneaux, puis un rafraîchissement. Sortie :
// JSON {load_ms, refresh_ms, panels, errors: [{title, message}]} ; code 1 si un panneau est en
// erreur ou si le rendu n'aboutit pas.
import { chromium } from 'playwright-core';

const [kibana, id, from = 'now-15m', to = 'now', shot, kql] = process.argv.slice(2);
if (!kibana || !id) {
  console.error('usage: node render.mjs <kibana> <dashboard> [from] [to] [screenshot.png] [KQL query]');
  process.exit(2);
}
// chaînes rison : ' et ! échappés par ! (plage de temps et requête de la barre de recherche)
const rison = (s) => s.replace(/[!']/g, '!$&');
const query = kql ? `&_a=(query:(language:kuery,query:'${encodeURIComponent(rison(kql))}'))` : '';
const url = `${kibana}/app/dashboards#/view/${id}?_g=(time:(from:'${rison(from)}',to:'${rison(to)}'),refreshInterval:(pause:!t,value:0))${query}`;

// Le conteneur annonce son nombre de panneaux ; chaque panneau rendu porte
// data-render-complete (mécanisme dont dépend le reporting de Kibana).
function rendered() {
  const root = document.querySelector('[data-shared-items-count]');
  if (!root) return false;
  const n = Number(root.getAttribute('data-shared-items-count'));
  return n > 0 && document.querySelectorAll('[data-shared-item][data-render-complete="true"]').length >= n;
}

// Bac à sable Chromium actif par défaut ; KB_NO_SANDBOX=1 le désactive là où il
// ne peut pas démarrer (exécution en root, conteneur sans espaces de noms utilisateur).
const browser = await chromium.launch({ args: process.env.KB_NO_SANDBOX ? ['--no-sandbox'] : [] });
let code = 0;
try {
  const page = await browser.newPage({ viewport: { width: 1600, height: 2400 } });
  // même délai que le rendu : à froid, les bundles de Kibana dépassent les 30 s par défaut
  await page.goto(url, { timeout: 120000 });
  await page.waitForFunction(rendered, null, { timeout: 120000 });
  const start = Date.now();
  await page.reload();
  await page.waitForFunction(rendered, null, { timeout: 120000, polling: 50 });
  const loadMs = Date.now() - start;
  // Rafraîchissement : délai entre le clic sur Refresh et le nouveau rendu de
  // tous les panneaux (requêtes comprises, démarrage de Kibana exclu). Le
  // début est vu par un observateur des attributs data-render-complete (un
  // panneau repasse à « false » même entre deux sondages) et par le sondage.
  const refreshMs = await page.evaluate((timeout) => new Promise((resolve, reject) => {
    const n = Number(document.querySelector('[data-shared-items-count]').getAttribute('data-shared-items-count'));
    const items = () => [...document.querySelectorAll('[data-shared-item]')];
    let started = false;
    const observer = new MutationObserver((mutations) => {
      if (mutations.some((m) => m.target.getAttribute('data-render-complete') !== 'true')) started = true;
    });
    for (const e of items()) observer.observe(e, { attributes: true, attributeFilter: ['data-render-complete'] });
    const t0 = performance.now();
    const timer = setInterval(() => {
      const done = items().filter((e) => e.getAttribute('data-render-complete') === 'true').length;
      if (done < n) started = true;
      if (started && done >= n) {
        clearInterval(timer);
        observer.disconnect();
        resolve(Math.round(performance.now() - t0));
      }
      if (performance.now() - t0 > timeout) {
        clearInterval(timer);
        observer.disconnect();
        reject(new Error(started ? 'refresh incomplete' : 'no panel refreshed'));
      }
    }, 5);
    document.querySelector('[data-test-subj="querySubmitButton"]').click();
  }), 120000);
  // laisse aux messages d'erreur asynchrones le temps de s'afficher
  await page.waitForTimeout(1000);
  // Erreurs : bloc d'erreur à la place du panneau, ou badge rouge de Lens
  // (champ introuvable, requête refusée…) dont le détail est dans une bulle.
  const panels = await page.$$('[data-test-subj="dashboardPanel"]');
  const errors = [];
  for (const p of panels) {
    const title = await p.$eval('[data-test-subj="embeddablePanelTitle"], [data-test-subj="dashboardPanelTitle"]',
      (e) => e.textContent.trim()).catch(() => '(untitled)');
    const block = await p.$('[data-test-subj="embeddableError"], [data-test-subj="embeddableStackError"], [data-test-subj="errorMessageMarkdown"]');
    if (block) {
      errors.push({ title, message: (await block.innerText()).slice(0, 300) });
      continue;
    }
    const badge = await p.$('[data-test-subj="lens-message-list-trigger"][class*="danger"]');
    if (badge) {
      await badge.click();
      const bubble = await page.waitForSelector('[data-popover-open] , .euiPopover__panel', { timeout: 5000 }).catch(() => null);
      const message = bubble ? (await bubble.innerText()).replace(/^You are in a dialog.*\n+/, '') : 'Lens error badge';
      errors.push({ title, message: message.slice(0, 300) });
      await page.keyboard.press('Escape');
      await page.waitForSelector('[data-popover-open], .euiPopover__panel', { state: 'detached', timeout: 5000 }).catch(() => {});
    }
  }
  const result = { panels: panels.length, errors };
  if (shot) await page.screenshot({ path: shot, fullPage: true });
  console.log(JSON.stringify({ load_ms: loadMs, refresh_ms: refreshMs, ...result }));
  if (result.errors.length > 0) code = 1;
} catch (e) {
  console.log(JSON.stringify({ error: String(e) }));
  code = 1;
} finally {
  await browser.close();
}
process.exit(code);
