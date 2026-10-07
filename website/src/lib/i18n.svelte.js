//! 站点的 i18n 运行时（照 luaur 站点的做法：扁平点分 key + en 铺底 + 逐条回退）
//!
//! 界面语言不进 URL 路径，只在启动时读 ?lang=，其次 localStorage，再次浏览器语言；
//! 词典按需 import() 并缓存，切语言不重载页面。

import { CODE } from "./locales/CODE.js";
import { NAME } from "./locales/NAME.js";

const STORAGE_KEY = "wedb_user_lang";
const FALLBACK = "en";

// 模板串动态 import：构建期由 Vite 展开成 locales 目录的模块图，
// 新增语言只改 CODE.js，这里不用同步维护（i18nCheck.js 负责核对文件齐不齐）
const loaders = Object.fromEntries(
  CODE.map((code) => [code, () => import(`./locales/${code}.js`)])
);

/// 各语言的自称，按 CODE.js 的顺序配对
export const LANGUAGES = CODE.map((code, index) => ({ code, name: NAME[index] }));

export const i18n = $state({ lang: FALLBACK, dict: {}, ready: false });

/// 简体语境下的 zh 写成 zh-CN，其余按语言码原样；zh-TW 本身就是合法的 BCP 47 标签
const HTML_LANG = { zh: "zh-CN" };

const ALIAS = {
  "zh-cn": "zh",
  "zh-sg": "zh",
  "zh-my": "zh",
  "zh-hans": "zh",
  "zh-hans-cn": "zh",
  "zh-tw": "zh-TW",
  "zh-hk": "zh-TW",
  "zh-mo": "zh-TW",
  "zh-hant": "zh-TW",
  "zh-hant-hk": "zh-TW",
  "zh-hant-tw": "zh-TW",
};

/// 把 navigator / ?lang= / localStorage 里的任意写法归一到 CODE.js 里的语言码，认不出返回 null
export function langNormalize(raw) {
  if (!raw) return null;
  const lower = String(raw).trim().toLowerCase();
  if (ALIAS[lower]) return ALIAS[lower];
  const exact = CODE.find((code) => code.toLowerCase() === lower);
  if (exact) return exact;
  return CODE.find((code) => code.toLowerCase() === lower.split("-")[0]) ?? null;
}

function langFromUrl() {
  try {
    return langNormalize(new URL(window.location.href).searchParams.get("lang"));
  } catch {
    return null;
  }
}

function langFromStorage() {
  try {
    return langNormalize(window.localStorage.getItem(STORAGE_KEY));
  } catch {
    return null;
  }
}

function langFromNavigator() {
  const candidates = navigator.languages ?? [navigator.language];
  for (const candidate of candidates) {
    const matched = langNormalize(candidate);
    if (matched) return matched;
  }
  return null;
}

/// 优先级：?lang= > 用户上次选择 > 浏览器语言 > en
export function detectLanguage() {
  return langFromUrl() ?? langFromStorage() ?? langFromNavigator() ?? FALLBACK;
}

const cache = new Map();

async function dictLoad(lang) {
  async function one(code) {
    if (cache.has(code)) return cache.get(code);
    const loader = loaders[code] ?? loaders.en;
    const module = await loader();
    const dict = module.default ?? module;
    cache.set(code, dict);
    return dict;
  }
  if (lang === "en") return one("en");
  // en 铺底再覆盖：某条词条在这个语言里漏了，退化成英文而不是露出裸 key
  const [base, current] = await Promise.all([one("en"), one(lang)]);
  return { ...base, ...current };
}

export function t(key) {
  return i18n.dict[key] ?? key;
}

/// 带占位符的词条：tf("hero.headline", {bulk: "5M"}) —— 语序由每种语言自己排
export function tf(key, vars) {
  return t(key).replace(/\{(\w+)\}/g, (match, name) =>
    name in vars ? String(vars[name]) : match
  );
}

/// 表格行名：本地化词条优先，缺词条时退回工厂端产出的 name（表体文字的最终权威仍是 Rust）
export function rowLabel(row) {
  return i18n.dict[`bench.row.${row.key}`] ?? row.name;
}

export async function languageSet(target, user_action = false) {
  const lang = langNormalize(target) ?? FALLBACK;
  const dict = await dictLoad(lang);
  i18n.lang = lang;
  i18n.dict = dict;
  document.documentElement.lang = HTML_LANG[lang] ?? lang;
  if (dict["meta.title"]) document.title = dict["meta.title"];
  const description = document.querySelector('meta[name="description"]');
  if (description && dict["meta.description"]) {
    description.setAttribute("content", dict["meta.description"]);
  }
  // 只在用户主动切换时记选择：自动匹配浏览器语言不该被固化成一次性的偏好
  if (user_action) {
    try {
      window.localStorage.setItem(STORAGE_KEY, lang);
    } catch {
      // 隐私模式下写 localStorage 会抛，站点照常工作
    }
  }
  return lang;
}

export async function i18nInit() {
  await languageSet(detectLanguage());
  i18n.ready = true;
}
