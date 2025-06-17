#!/usr/bin/env python3
import json
import requests
import base58
import requests
# ─── CONFIG ─────────────────────────────────────────────────────────────────────
RPC_URL      = "http://127.0.0.1:9876"
RPC_USER     = "rpc"
RPC_PASSWORD = "rpc"
FEE_RATE     = 1_000_000   # sats per kB = 0.01 B1T/kB
# ────────────────────────────────────────────────────────────────────────────────

def rpc(method, params=None):
    payload = {"jsonrpc": "1.0", "id": "sender", "method": method, "params": params or []}
    r = requests.post(RPC_URL, auth=(RPC_USER, RPC_PASSWORD),
                      headers={"Content-Type": "application/json"},
                      data=json.dumps(payload))
    r.raise_for_status()
    rep = r.json()
    if rep.get("error"):
        raise RuntimeError(json.dumps(rep["error"]))
    return rep["result"]


def list_utxos(addr: str):
    """
    Query blockbook.b1tcore.org for both confirmed and unconfirmed UTXOs.
    Returns a list of dicts: {txid, vout, sats}.
    """
    # confirmed=false means “include mempool UTXOs too”
    url = f"https://blockbook.b1tcore.org/api/v2/utxo/{addr}?confirmed=false"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()  # a list of UTXO objects
    utxos = []
    for u in data:
        # value is a string of satoshis per Blockbook spec
        sats = int(u["value"])
        utxos.append({
            "txid": u["txid"],
            "vout": u["vout"],
            "sats": sats
        })
    return utxos


def estimate_fee(n_in: int, n_out: int) -> int:
    size = 180 * n_in + 34 * n_out + 10
    return (FEE_RATE * size + 999) // 1000


def build_tx(wif, inputs, outputs):
    raw = rpc("createrawtransaction", [inputs, outputs])
    signed = rpc("signrawtransaction", [raw, [], [wif]])
    if not signed.get("complete"):
        raise RuntimeError("Signing failed")
    return signed["hex"]


def send_rb1ts(
    wif: str,
    addr: str,
    total_rb: float,
    send_rb: float,
    utxo_index: int,
    recv_addr: str,
    rest_addr: str,
    change_addr: str
):
    """
    Build and sign a RB1TS transaction using the same logic as the CLI.
    Returns (txhex, metadata).
    """
    utxos = list_utxos(addr)
    if not utxos:
        raise ValueError("❌ no UTXOs to spend")

    base = utxos[utxo_index]

    info = rpc("getnetworkinfo")
    soft_limit = info.get("softdustlimit", info.get("harddustlimit"))
    DUST_LIMIT = int(soft_limit * 1e8)

    send_sats = DUST_LIMIT
    total_required = int((total_rb / send_rb) * send_sats)

    # Detect full-balance send
    full_send = (total_rb == send_rb)
    outputs_count = 2 if full_send else 3

    # Coin selection
    selected = [base]
    acc = base["sats"]
    fee = estimate_fee(len(selected), outputs_count)
    needed = total_required + fee
    others = [u for u in utxos if u != base]
    others.sort(key=lambda x: x["sats"], reverse=True)
    for u in others:
        if acc >= needed:
            break
        selected.append(u)
        acc += u["sats"]
        fee = estimate_fee(len(selected), outputs_count)
        needed = total_required + fee

    if acc < needed:
        raise ValueError(f"❌ insufficient funds: need {needed} sats but have {acc}")
    total_inputs = acc

    # Outputs mapping
    recv_sats = send_sats
    rest_sats = total_required - recv_sats
    change_sats = total_inputs - total_required - fee

    inputs = [{"txid": u["txid"], "vout": u["vout"]} for u in selected]

    if full_send:
        # only receiver + change
        outputs = {
            recv_addr:   recv_sats   / 1e8,
            change_addr: change_sats / 1e8,
        }
    else:
        # receiver + rest + change
        outputs = {
            recv_addr:   recv_sats    / 1e8,
            rest_addr:   rest_sats    / 1e8,
            change_addr: change_sats  / 1e8,
        }

    txhex = build_tx(wif, inputs, outputs)
    metadata = {
        "selected_utxos": selected,
        "fee": fee,
        "total_required": total_required,
        "total_inputs": total_inputs,
        "recv_sats": recv_sats,
        "rest_sats": rest_sats,
        "change_sats": change_sats,
        "outputs_count": outputs_count,
    }
    return txhex, metadata


def broadcast_tx(txhex: str) -> str:
    """Broadcast raw TX hex and return TXID."""
    return rpc("sendrawtransaction", [txhex])


def interactive_main():
    print("\n== RB1TS Sender (3 outputs with base-UTXO selection) ==")
    wif  = input("Your PRIVATE WIF (not stored): ").strip()
    addr = input("Your B1T address (holds sats): ").strip()

    # 1) fetch UTXOs & display
    utxos = list_utxos(addr)
    if not utxos:
        return print("❌ no UTXOs to spend")
    print("Available UTXOs:")
    for i,u in enumerate(utxos):
        print(f"  [{i}] {u['txid']}:{u['vout']} → {u['sats']} sats")

    # 2) user’s RB1TS holdings & send amount
    total_rb = float(input("How many total RB1TS tokens do you hold? ").strip())
    send_rb  = float(input("How many RB1TS tokens to send? ").strip())
    if send_rb > total_rb:
        return print("❌ cannot send more tokens than you hold")

    # 3) choose base UTXO that carries RB1TS
    idx = int(input("Select index of UTXO with your RB1TS tokens: ").strip())
    base = utxos[idx]

    # 4) derive network dust limit
    info = rpc("getnetworkinfo")
    soft_limit = info.get("softdustlimit", info.get("harddustlimit"))
    DUST_LIMIT = int(soft_limit * 1e8)
    print(f"Using network dust limit = {DUST_LIMIT} sats")

    # 5) sats mapping: X = DUST_LIMIT for send_rb tokens
    send_sats = DUST_LIMIT
    total_required = int((total_rb / send_rb) * send_sats)

    # 6) coin selection: always include base, then add others to cover total_required + fee
    selected = [base]
    acc = base["sats"]
    fee = estimate_fee(len(selected), 3)
    needed = total_required + fee
    others = [u for u in utxos if u != base]
    # sort descending to minimize inputs
    others.sort(key=lambda x: x["sats"], reverse=True)
    for u in others:
        if acc >= needed:
            break
        selected.append(u)
        acc += u["sats"]
        fee = estimate_fee(len(selected), 3)
        needed = total_required + fee
    if acc < needed:
        return print(f"❌ insufficient funds: need {needed} sats but have {acc}")
    total_inputs = acc

    # 7) compute outputs
    recv_sats = send_sats
    rest_sats = total_required - recv_sats
    change_sats = total_inputs - total_required - fee

    # 8) collect addresses
    recv_addr = input(f"Recipient address (gets {recv_sats} sats): ").strip()
    rest_addr = input(f"Rest address (gets {rest_sats} sats): ").strip()
    change_addr = input(f"Change address (gets {change_sats} sats): ").strip()

    # 9) build transaction
    inputs = [{"txid": u["txid"], "vout": u["vout"]} for u in selected]
    outputs = {
        recv_addr:   recv_sats / 1e8,
        rest_addr:   rest_sats / 1e8,
        change_addr: change_sats / 1e8,
    }

    print("\nBuilding & signing transaction…")
    txhex = build_tx(wif, inputs, outputs)
    print("Raw TX hex:", txhex)

    # 10) broadcast
    if input("Broadcast? [y/N] ").lower().startswith("y"):
        try:
            txid = rpc("sendrawtransaction", [txhex])
            print("✅ Broadcasted as", txid)
        except requests.exceptions.HTTPError as e:
            print("RPC HTTP error:", e.response.status_code)
            print("Response body:", e.response.text)
        except RuntimeError as e:
            print("RPC returned error:", e)
    else:
        print("🛑 Not broadcast.")

if __name__ == "__main__":
    interactive_main()
