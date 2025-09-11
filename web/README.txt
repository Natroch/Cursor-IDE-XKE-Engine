Quick start
-----------
1) Put your deployed contract address into a small shim in index.html (optional):
   <script>window.CONTRACT_ADDRESS = "0xPASTE_DEPLOYED_CONTRACT";</script>
   Insert this above the app.js script tag.

   Alternatively, after loading the page, paste in the address using the devtools console:
   window.CONTRACT_ADDRESS = "0x..."; location.reload();

2) Serve locally:
   powershell> cd web
   powershell> python -m http.server 8080

3) Open http://localhost:8080 in your browser, click "Connect Wallet" (MetaMask), and use the UI to:
   - Read owner
   - Read USDT balance held by the contract
   - Call recoverEcho(recipient, amount)

Notes
-----
- The connected wallet must be the contract owner to call recoverEcho.
- Amount is in human USDT (e.g., 10.00). The app converts to token units.
- The page uses ethers v6 via CDN; no build step is required.
