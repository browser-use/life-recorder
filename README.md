# Life Recorder

A native iPhone recorder and private Mac receiver. The iPhone records approximately one-minute AAC chunks. A Mac receiver transcribes them locally with Whisper and maintains one continuous Markdown transcript with hourly markers. Audio is deleted after durable receipt and successful transcription. No paid transcription service or cloud backend is required.

## Requirements

- macOS with Xcode and an Apple developer account capable of installing a development build on the iPhone
- iPhone running a supported iOS version, with Developer Mode enabled for development installation
- Python 3.10+, `ffmpeg`, `whisper-cli` from whisper.cpp, and a downloaded GGML Whisper model
- A reachable HTTPS path between phone and Mac (same LAN by default; use a private VPN for cellular access)

## Build and install

Open `ios/LifeRecorder.xcodeproj` in Xcode, select the connected iPhone, choose your Apple development team, and Build and Run. The app requests microphone and local-network access. Keep the source tree free of runtime credentials.

For command-line builds, use an Apple development team and the connected device identifier:

```sh
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer \
  xcodebuild -project ios/LifeRecorder.xcodeproj -scheme LifeRecorder \
  -destination 'id=YOUR_DEVICE_UDID' -allowProvisioningUpdates \
  DEVELOPMENT_TEAM=YOUR_TEAM_ID build
```

## Configure the Mac receiver

```sh
python3 receiver/setup.py \
  --data-dir /absolute/private/runtime \
  --model /absolute/path/to/ggml-small.bin \
  --install-agent
```

Setup creates a random bearer token, a self-signed TLS certificate, and a private pairing page in the data directory. Open that page only on the intended iPhone. The token is stored in the iPhone Keychain and in the private Mac runtime; it is ignored by Git. The receiver binds an authenticated upload endpoint and does not expose transcript downloads or arbitrary Mac access.

The receiver writes the combined transcript to `life.md` in the data directory. It can be placed anywhere, including `~/Documents/life.md`, by moving that file and leaving a symlink at the runtime path.

## Recording behavior

Tap the recorder switch once. Recording continues while the screen is locked and while other apps are used. If the iPhone is rebooted or the app is force-quit, iOS requires opening Life Recorder once before microphone capture can resume. Pending audio remains on the phone until the receiver acknowledges it. Upload tasks are retried and stale connectivity tasks are cancelled so they cannot hold the queue indefinitely.

Whisper runs locally on the Mac. The receiver strips Whisper stage-direction captions — parenthetical and bracketed sound labels, plus lyric markers — and highly repetitive hallucinated noise, then writes one continuous document with an hourly capture marker. This is cleanup, not a guarantee of perfect transcription. A clip that is only captions is dropped.

## Using Codex to reproduce the setup

The accompanying `SKILL.md` is a reusable Codex procedure. Codex can inspect and edit this source, build it with Xcode, and use Apple CoreDevice tooling to install and launch it on a connected iPhone. For visual phone interaction it uses the CUA iPhone Mirroring surface. Codex must not guess or bypass the iPhone passcode; the user handles protected prompts, trust dialogs, Developer Mode, and microphone/local-network approval. Codex should never print or commit runtime tokens, private keys, pairing pages, audio, transcripts, device identifiers, or user-specific paths.

## Security and limits

The default connection is local-LAN HTTPS with certificate pinning and a 256-bit random bearer token. The receiver has no public tunnel or cloud storage built in. A private VPN is required for cellular uploads outside the home network. Anyone who can read the private runtime directory can read the token and transcript, so keep that directory private and out of backups or repositories as appropriate.

## License

MIT. See [LICENSE](LICENSE).
