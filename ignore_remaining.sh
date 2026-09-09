#!/bin/bash
find "check/miss" -name "*.yml" | while read file; do
  rel_path=${file#check/miss/}
  mkdir -p "js/check/ignore/$(dirname "$rel_path")"
  cp "$file" "js/check/ignore/$rel_path"
done
