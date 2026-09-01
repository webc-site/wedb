#!/usr/bin/env -S bun
import {
  reportGenerateFromYaml,
  benchMarkdownRender,
  qpsFormat,
  usFormat,
} from "./bench_report.mjs";

export {
  reportGenerateFromYaml,
  benchMarkdownRender,
  qpsFormat,
  usFormat,
  reportGenerateFromYaml as reportGenerateFromYamlFunc,
  reportGenerateFromYaml as generateReportFromYaml,
};

export default reportGenerateFromYaml;

if (import.meta.main) {
  await reportGenerateFromYaml();
}

