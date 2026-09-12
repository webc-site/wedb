import { optimize } from "svgo";
import cdnUpload from "@1-/github_cdn";

const ENCODER = new TextEncoder(),
  DECODER = new TextDecoder();

export const svgOptimize = (svg_input) => {
    const raw_str =
        typeof svg_input === "string"
          ? svg_input
          : DECODER.decode(svg_input),
      res = optimize(raw_str, {
        multipass: true,
      });
    return res.data;
  },
  ghToken = () => process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN ?? "",
  svgUpload = async (svg_input) => {
    const token = ghToken();
    if (!token) return "";

    const raw_str =
        typeof svg_input === "string"
          ? svg_input
          : DECODER.decode(svg_input),
      opt_svg = svgOptimize(raw_str),
      raw_len = ENCODER.encode(raw_str).length,
      opt_len = ENCODER.encode(opt_svg).length;

    if (raw_len > opt_len) {
      const saved_pct = (100 - (opt_len / raw_len) * 100).toFixed(1);
      console.log("  -> SVGO 压缩: " + raw_len + " B -> " + opt_len + " B (减小 " + saved_pct + "%)");
    }

    const upload_buf = ENCODER.encode(opt_svg),
      upload = cdnUpload(token, "webc-fs/-"),
      raw_url = await upload(upload_buf, "svg"),
      full_url = raw_url.startsWith("//") ? "https:" + raw_url : raw_url,
      { pathname } = new URL(full_url);

    return "https://fastly.jsdelivr.net" + pathname;
  },
  optimizeSvg = svgOptimize,
  uploadSvg = svgUpload;

export default svgUpload;
