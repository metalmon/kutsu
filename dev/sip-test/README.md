# Local SIP test PBX (Asterisk + Linphone)

A throwaway, provider-free way to exercise kutsu's SIP leg **SIP↔SIP** — no PSTN,
no cost, no contract. Asterisk runs in Docker as a tiny PBX; Linphone plays the
callee; kutsu (once its SIP module lands) registers and places calls.

- Endpoints: `kutsu` / `kutsupw` (the caller under test) and `callee` / `calleepw`
  (Linphone). Dev-only credentials.
- Codecs: **G.711 ulaw/alaw only** — the same codec kutsu will bridge on the
  phone side, so this validates the real media path.
- Dialplan (context `internal`):
  - `600` — echo test (Asterisk echoes your audio back; best first check).
  - `601` — plays a built-in prompt (one-way audio).
  - `602` — 1 kHz test tone.
  - `6001` — calls the Linphone softphone (real two-way call).

## Windows: run Asterisk in WSL2, NOT Docker (important)

Docker Desktop on Windows **cannot route RTP** for this. SIP signaling works
(registration, call setup), but audio is silent: containers run in Docker's
internal VM (`192.168.65.x` / bridge `172.20.x`), so Asterisk puts an
unreachable container IP in the SDP `c=` line and `external_media_address`
does not fix it for a same-host softphone. `network_mode: host` is a no-op on
Docker Desktop (the container lands on the VM subnet, not the LAN). This is a
known Docker-Desktop limitation, not an Asterisk misconfig.

**On Windows, run Asterisk natively in WSL2** (with `networkingMode=Mirrored`
in `%USERPROFILE%\.wslconfig`, the WSL2 distro shares the host's LAN IP, so
SDP/RTP are correct). In a WSL Ubuntu terminal:

```bash
sudo apt update && sudo apt install -y asterisk
SRC=/mnt/<drive>/…/kutsu/dev/sip-test/asterisk
sudo cp "$SRC/extensions.conf" "$SRC/rtp.conf" /etc/asterisk/
# WSL is on the real LAN, so drop the NAT external_* lines:
sudo sed -E 's/^(external_media_address|external_signaling_address|local_net)/;\1/' \
  "$SRC/pjsip.conf.example" | sudo tee /etc/asterisk/pjsip.conf >/dev/null
sudo systemctl restart asterisk 2>/dev/null || sudo service asterisk restart
sudo asterisk -rx "pjsip show endpoints"
```

Then point the softphone at the host's LAN IP (e.g. `192.168.88.243:5060`).

**Softphone gotchas (bit us):**
- A desktop softphone (Linphone, MicroSIP) squats local UDP **5060** by
  default. If Asterisk is also on 5060 on the same host, the softphone can't
  bind and hangs on "connecting". Set the softphone's *local* SIP port to
  something else (e.g. random / 5070), or run only one softphone at a time.
- Linphone Desktop's account config is opaque (accounts aren't always saved to
  `linphonerc`). **MicroSIP** is far easier: Domain `<host-ip>:5060`, user
  `callee`, pass `calleepw`, transport UDP.

The Docker setup below still works on **Linux** (where `network_mode: host` is
real). On Windows, prefer the WSL2 path above.

## 1. Docker networking mode (Linux; pick one)

SIP + RTP are UDP and NAT-sensitive. Two options:

First create your local PBX config from the template (the real `pjsip.conf`
carries your LAN IP and is gitignored):

```bash
cd dev/sip-test
cp asterisk/pjsip.conf.example asterisk/pjsip.conf
```

Then pick a networking mode:

- **Published ports (default; Docker Desktop on Windows/macOS):** the compose
  file maps `5060/udp` + `10000-10020/udp`. Set your host's LAN IP in
  `asterisk/pjsip.conf` — the three `external_*`/`local_net` lines (find it with
  `ipconfig` on Windows). Linphone and kutsu must advertise that **same LAN IP**,
  not `127.0.0.1`, so Asterisk-relayed RTP reaches them.
  _(On this checkout `pjsip.conf` was already created with `192.168.88.243`.)_
- **Host networking (Linux / WSL2):** simplest — no NAT games. In
  `docker-compose.yml` comment out the `ports:` block and uncomment
  `network_mode: host`; in `pjsip.conf` comment out the three
  `external_*`/`local_net` lines.

## 2. Start Asterisk

```bash
cd dev/sip-test
docker compose up -d
docker compose logs -f          # watch it boot
```

Useful checks (Asterisk CLI inside the container):

```bash
docker exec -it kutsu-asterisk asterisk -rx "pjsip show endpoints"
docker exec -it kutsu-asterisk asterisk -rx "pjsip show registrations"
```

Edited a `.conf`? Reload without restart:

```bash
docker exec -it kutsu-asterisk asterisk -rx "core reload"
```

## 3. Register Linphone as the callee

Desktop Linphone (same PC) or the phone app both work — see "Desktop vs phone"
below. In Linphone → add a SIP account:

- Username: `callee`
- Password: `calleepw`
- Domain / SIP proxy: `192.168.88.243:5060`
- Transport: UDP

Then, from Linphone, dial `600` — you should hear your own voice echoed back.
That confirms registration + RTP + G.711 end to end.

### Desktop vs phone

- **Desktop Linphone (same machine):** least friction, recommended for a first
  check. Use the domain `192.168.88.243:5060` above.
- **Phone Linphone app:** also fine, and a nicer real-device test — but the
  phone must be on the **same Wi-Fi/LAN** (a `192.168.88.x` address), and
  Windows Firewall must allow inbound UDP to Docker. Add the rule once, in an
  **admin** PowerShell:

  ```powershell
  New-NetFirewallRule -DisplayName "kutsu-sip-test" -Direction Inbound `
    -Protocol UDP -LocalPort 5060,10000-10020 -Action Allow
  ```

  Then in the phone app use the same account (`callee` / `calleepw`, domain
  `192.168.88.243:5060`, UDP).

## 4. Point kutsu at it (when the SIP leg exists)

kutsu's SIP module (phase 1/3) isn't implemented yet — `src/sip.rs` is still a
stub. Once it can REGISTER + INVITE, register it as `kutsu` / `kutsupw` against
`<HOST_IP>:5060` and have it dial:

- `600` — echo, to validate kutsu's audio bridge (send G.711, get it back).
- `6001` — to ring the Linphone softphone for a real two-way test.

Until then, you can validate the PBX itself with two Linphone accounts, or
Linphone → `600`/`601`.

## Notes / gotchas

- **Advertise the LAN IP, not loopback.** The #1 cause of "call connects but no
  audio" here is a client putting `127.0.0.1` in its SDP while Asterisk runs in a
  container — the RTP then goes nowhere. Use the host LAN IP everywhere (or host
  networking).
- `direct_media=no` keeps Asterisk relaying RTP, which is what makes the
  host↔container case work; don't switch it on for this local setup.
- The RTP port range in `rtp.conf` and the published range in
  `docker-compose.yml` must match.
- Throwaway/dev only — plaintext passwords, no TLS/SRTP. Not for anything real.
