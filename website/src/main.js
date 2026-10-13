import { mount } from "svelte";
import "./app.css";
import App from "./App.svelte";
import { i18nInit } from "./lib/i18n.svelte.js";

// 词典先到手再挂载：否则首帧会闪一下裸 key（i18n 只拉当轮语言 + en 铺底，不阻塞成白屏）
await i18nInit();

const app = mount(App, { target: document.getElementById("app") });

export default app;
