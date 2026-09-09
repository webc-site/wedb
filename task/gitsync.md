1.
进入 garnet 记录当前版本
然后，拉取
ssh://git@ssh.github.com:443/microsoft/garnet.git

查看 diff

如改变较多，按时间拆分提交，每次合并一天的 diff

2. 我们对标 garnet c# 实现了它的 rust 版本

在 /tmp/fork 开 worktree，然后

让子代理参考上面的 diff，优化 rust 代码，修复问题，补全测试

然后运行测试

3. 开一个全新的子代理，对照 diff 和 rust 的修改，code review，并让它修复问题，优化代码

代码标准，参考 ./sh/skills/rust_review/SKILL.md

如此循环，不断 code review，直到子代理认为代码完美，达到工业级标准

4. 合并 worktree

如主库有改动，自动提交，无需确认

提交，推送，合并到 main，推送

清理删除 worktree

5. 进入 garnet， 合并 microsoft 的改动，推送