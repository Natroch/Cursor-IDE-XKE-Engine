const CONFIG = {
  rpcUrl: "https://eth.llamarpc.com",
  chainId: 1,
  // Set these after deploy; for read-only you can keep the defaults
  contractAddress: (window.CONTRACT_ADDRESS || null),
  usdtDecimals: 6,
};

// Minimal ABI for recoverEcho(recipient,address) and read helpers
const CONTRACT_ABI = [
  { "type": "function", "name": "owner", "inputs": [], "outputs": [{"type":"address"}], "stateMutability": "view" },
  { "type": "function", "name": "usdtBalance", "inputs": [], "outputs": [{"type":"uint256"}], "stateMutability": "view" },
  { "type": "function", "name": "recoverEcho", "inputs": [
      {"name":"recipient","type":"address"},
      {"name":"amount","type":"uint256"}
    ], "outputs": [], "stateMutability": "nonpayable" },
];

let provider, signer, contract;

function fmt(v) { return typeof v === 'string' ? v : String(v); }
function byId(id) { return document.getElementById(id); }

async function connect() {
  const status = byId('connStatus');
  try {
    if (!window.ethereum) {
      status.textContent = 'No wallet. Install MetaMask.';
      status.className = 'err';
      return;
    }
    provider = new ethers.BrowserProvider(window.ethereum);
    await provider.send('eth_requestAccounts', []);
    signer = await provider.getSigner();
    const addr = await signer.getAddress();
    const net = await provider.getNetwork();
    byId('addr').textContent = addr;
    byId('chain').textContent = fmt(net.chainId);
    status.textContent = 'Connected';
    status.className = 'ok';

    if (!CONFIG.contractAddress) {
      byId('contract').textContent = 'Not configured';
      return;
    }
    contract = new ethers.Contract(CONFIG.contractAddress, CONTRACT_ABI, signer);
    byId('contract').textContent = CONFIG.contractAddress;
    await refreshState();
  } catch (e) {
    status.textContent = `Error: ${e.message || e}`;
    status.className = 'err';
  }
}

async function refreshState() {
  if (!contract) return;
  try {
    const [owner, balRaw] = await Promise.all([
      contract.owner(),
      contract.usdtBalance(),
    ]);
    byId('owner').textContent = owner;
    const bal = Number(balRaw) / (10 ** CONFIG.usdtDecimals);
    byId('bal').textContent = `${bal.toLocaleString(undefined,{maximumFractionDigits:6})} USDT`;
  } catch (e) {
    byId('bal').textContent = `Error: ${e.message || e}`;
    byId('bal').className = 'err';
  }
}

async function recover() {
  const out = byId('txStatus');
  out.textContent = '-'; out.className = 'muted';
  if (!contract) { out.textContent = 'Contract not configured'; out.className='err'; return; }
  try {
    const recipient = byId('recipient').value.trim();
    const amountStr = byId('amount').value.trim();
    if (!recipient || !amountStr) { out.textContent = 'Enter recipient and amount'; out.className='warn'; return; }
    const amountUnits = ethers.parseUnits(amountStr, CONFIG.usdtDecimals);
    const tx = await contract.recoverEcho(recipient, amountUnits);
    out.textContent = `Submitted: ${tx.hash}`;
    out.className = 'ok';
    const receipt = await tx.wait();
    if (receipt.status === 1) {
      out.textContent = `Confirmed: ${tx.hash}`;
      out.className = 'ok';
      await refreshState();
    } else {
      out.textContent = `Failed: ${tx.hash}`;
      out.className = 'err';
    }
  } catch (e) {
    out.textContent = `Error: ${e.message || e}`;
    out.className = 'err';
  }
}

byId('btnConnect').addEventListener('click', connect);
byId('btnRefresh').addEventListener('click', refreshState);
byId('btnRecover').addEventListener('click', recover);

