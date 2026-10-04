# go-worker 功能實作與驗證

日期：2026-10-03。範圍：Go 1.27.1、單程序、記憶體內 worker pool。
原評估中的五項建議已實作；既有 `Task` 介面及同步 API 的 sentinel 回傳方式保留。

## 實作結果

| 功能 | 公開 API | 行為與範圍 |
| --- | --- | --- |
| 可取消排程 | `ScheduleContext`、`DoContext` | 取消取得 worker／交付前的等待；交付後等真實執行結果，避免提前歸還 worker |
| 等待關閉 | `Worker/Master.Shutdown(ctx)`、`Wait(ctx)` | 關閉接單並等待；Master 的 Shutdown 會處理完已接受的非同步佇列，Stop 則令未執行的 queued tasks 取得停止錯誤 |
| 結構化結果 | `ScheduleResult(ctx, task)`、`DoResult(ctx, task)`、`Result` | 回傳 task ID、失敗 phase、sentinel、原始 cause、panic value 與 stack；舊 API 的 `err == ErrWorkerTaskRun` 仍可使用 |
| Logger／狀態快照 | `WithLogger`、`WithMasterLogger`、`Worker/Master.Stats()` | 替換 worker 繼承 logger；統計成功、失敗、panic、執行中任務，Master 累計值跨 recovery 保留 |
| 有界非同步佇列 | `WithQueueCapacity`、`Submit(ctx, task)`、`TrySubmit(task)`、`Future.Wait/Done` | 阻塞等待容量或立即拒收；同一 future 可重複、並行讀取 |

`WithQueueCapacity(0)` 為預設，停用非同步提交。負容量、明確指定 nil logger 都會在
建構時回傳錯誤。Master 必須先加入、啟動 workers；若未啟動，執行結果會回報相應錯誤。

## 取消、容量與關閉的精確邊界

`ScheduleContext` 的交付與取消若同時可進行，兩者任一可勝出。回傳 context
取消錯誤時，任務尚未送出；交付成功後即等待結果。取消 context 不強制中止既有
`Init/Run/Done`。這遵循 context 發出通知與工作實際停止是不同事件的界線。
[Go context 文件](https://pkg.go.dev/context#CancelFunc)

`Submit` 的 context 只控制接納前的等待。接納成功後，任務獨立於此 context；
`Future.Wait` 的 context 也只取消該次觀察，之後仍可等待相同結果。任務錯誤在
`Result.Err`，觀察逾時則在 `Future.Wait` 的第二個回傳值。

佇列容量計入等待交付的隊頭。dispatcher 成功交付後才取出隊頭，避免先取出任務、
再建立無限制的 goroutine 去等待 worker。佇列交付依接納順序，完成順序可不同。
同步 `Schedule` 跳過此佇列，不承諾與非同步工作間的全域公平性。

`Shutdown` 拒絕新接單，喚醒尚未交付的同步等待者，讓已接納的非同步工作取得結果，
再等待 worker、queue forwarding、dispatcher 及內部 recovery goroutine 結束。
等待逾時不會終止背景 drain；可繼續 `Wait`，或 `Stop` 拒絕尚未執行的佇列工作。
已執行的任務若不自行返回，等待關閉只能逾時，無法強制殺掉該 goroutine。

應由 pool 擁有者呼叫並等待 `Shutdown`。任務內等待自己的 pool 關閉會等待自身；
向同一個已滿 pool 阻塞提交也可能循環等待，應改用 `TrySubmit`、deadline 或由外部提交。

## 統計、結果與直接 channel 使用

執行統計包含 `Started/Succeeded/Failed/Panicked/InFlight`，每份計數快照滿足：
`Started = Succeeded + Failed + Panicked + InFlight`。
交付前被拒絕的任務不計入；nil task 若直接從 channel 收到，則算一次執行失敗。
Master 的統計包含註冊後開始的直接 worker 任務，並跨替換 worker 累計。
佇列／pool 欄位與執行計數分別取樣，不宣稱整份 Master 快照為全域原子視圖。

`Result.Phase` 表示失敗的方法；成功或派發前拒絕時為空。`TaskID` 為最後一次成功讀到
的 ID；若第一次 `ID()` 就 panic，則為空。Panic 與 `runtime.Goexit()` 均產生終止結果，
不會留下永遠等待 result 的呼叫端。`Result.Cause` 可使用標準 `errors.Is/As` 檢查。

`Worker.Task` 相容介面仍保留，但直接送入會繞過 Master 的接單與 queue capacity。
每個 worker 應只由一個 Master 管理；執行中不要外部修改公開 channels、Name 或 Pool。
注入 logger 的最終 `Sync` 由呼叫端負責。

## 驗證證據

- 原有測試保留且通過。
- 新測試涵蓋取消與交付競爭、取消後名額歸還、交付後不提早釋放 worker、關閉時等待
  直接執行中的任務、並行 Stop/Shutdown、未啟動 pool 的關閉、滿載拒收、阻塞提交恢復、
  多 worker 非同步執行、future 多次／多人等待、FIFO 交付與 graceful drain 跨 panic。
- 驗證每階段 cause、ID panic、`runtime.Goexit()`、舊 `panicnil=1` 行為，以及 logger
  與統計跨 recovery 保留。
- 已重現並修正「最後一個 worker panic、recovery 停用時，既有排程等待者未被喚醒」：
  修正前回歸測試逾時，修正後透過 pool 變動通知回傳空 pool 錯誤，queued futures 也有結果。
- 公開 API 的使用方式另以 `example_test.go` 編譯並執行。
- 使用 Go 1.27.1 完成 `go build ./...`、`go vet ./...`，以及
  `go test -race -shuffle=on -count=20 ./...`，全部通過。
- `go mod tidy -diff` 無差異，`git diff --check` 通過。

知識圖譜已建立，`.codebase-memory/` 已加入 `.gitignore`。
圖譜 coverage 是結構性輔助；並行正確性依實際原始碼與測試核對，測試通過不代表
所有排程交錯或吞吐效能已獲證明。尚未推送，沒有遠端 GitHub Actions 通過的證據。

## 2026-10-04：恢復失敗回報與重啟上限

保留既有 panic 後即時替換 worker 的流程，新增 `RecoveryPolicy` 與
`Master.RecoveryStats()`。啟用 recovery 的預設上限為每個 worker 位置每分鐘
5 次替換，可透過 `WithRecoveryPolicy` 調整。計數跨替代 instance 累計，成功任務
或 `Start()` 成功都不重設計數；超出窗口的嘗試會從滾動窗口移除。

達到上限、建構或啟動替代 worker 失敗時，該位置退出服務；剩餘 workers 繼續接單。
上述失敗導致失去全部位置時，新接單及等待交付的任務會取得 `ErrRecoveryExhausted` 或
`ErrRecoveryFailed`，原 panic 任務仍取得 `ErrWorkerPanic`。位置不會隨窗口到期
自動復活；須新增並啟動 worker 恢復容量。`LastFailure` 保留原始錯誤、失敗階段、
worker／instance 身分與時間，計數快照滿足 `Attempts = Started + Failed`。
`Started` 僅代表替代 worker 已啟動並註冊，不表示已持續健康運作。

退出 observer 會補救被丟棄的 recovery 通知；與既有兩條恢復路徑使用相同 instance
身分去重。panic 任務結果先交付，再寫診斷 log，並攔住診斷 logger 的第二次 panic。
阻塞不返回的 logger 仍可能延後 worker 退出與 Shutdown。

新增故障測試涵蓋跨 instance 額度、各位置獨立計數、滾動窗口邊界、建構／啟動失敗
原始原因、等待中的排程／queue／阻塞 Submit 終局錯誤、graceful Shutdown 跨額度耗盡、
通知遺失、重複恢復、啟動連續 panic、logger 二次 panic／阻塞，以及 Stop 後不再重啟。
已重現「取得已 panic worker 的舊 token，替換失敗卻只回傳 worker stopped」；修正後
回傳恢復終局錯誤或繼續取得健康位置。公開 API 另有可執行範例。

本次完成 `go build ./...`、`go vet ./...` 及
`go test -race -shuffle=on -count=20 ./...`，全部通過；`git diff --check` 通過。
知識圖譜已重新產生並持續由 `.gitignore` 排除。

## 原評估中仍暫緩的方向

自動重試／backoff、優先佇列、動態縮容、rate limit、持久化及分散式任務處理未列入
本次五項實作。重試仍需先定義冪等性與副作用，容量與效能調整則需要實際負載證據。
