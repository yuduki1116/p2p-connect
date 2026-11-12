# p2p_connect v3 (TCP+UDP)
P2Pトンネル(iroh/QUIC、NAT越え+リレー自動フォールバック)。Tauri 2製。
- 共有側: ローカルのアドレスを指定 → 接続IDが出る
- 接続側: IDを入力 → localhost:入口ポート がトンネルの入口になる

## Windowsでローカルビルド
Rust, Node.js, WebView2 が必要
    npm install
    npx tauri icon app-icon.png
    npx tauri build
## GitHub Actions
リポジトリにpush → Actions → build を手動実行(またはvタグ)。Mac/Android/iOSもここでビルド。
