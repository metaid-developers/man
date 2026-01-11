#!/bin/bash

# MVC Test Script - Key Generation and Transfer Testing

set -e

echo "========================================="
echo "MVC Chain Testing Tool"
echo "========================================="
echo ""

# Compile tools
echo "1. Compiling test tool..."
cd cmd/mvctest
go build -o mvctest
if [ $? -ne 0 ]; then
    echo "❌ Compilation failed"
    exit 1
fi
echo "✓ Compilation successful"
echo ""

# Generate key pair
echo "2. Generating new key pair..."
./mvctest generate > keypair.txt
cat keypair.txt
echo ""

# Extract address information
MVC_ADDR=$(grep "MVC Address:" keypair.txt | awk '{print $3}')
ID_ADDR=$(grep "ID Address:" keypair.txt | awk '{print $3}')
PRIV_KEY=$(grep "Private Key (Hex):" keypair.txt | awk '{print $4}')

echo "========================================="
echo "Generated Address Information"
echo "========================================="
echo "MVC Address: $MVC_ADDR"
echo "ID Address:  $ID_ADDR"
echo ""

# Save to file
cat > test_account.txt <<EOF
MVC Test Account Information
Generated: $(date)

MVC Address: $MVC_ADDR
ID Address:  $ID_ADDR
Private Key: $PRIV_KEY

⚠️  This is a test account, do not use in production environment
EOF

echo "✓ Account information saved to test_account.txt"
echo ""

echo "========================================="
echo "Next Steps"
echo "========================================="
echo "1. Send test coins to this address:"
echo "   MVC Address: $MVC_ADDR"
echo ""
echo "2. Check balance:"
echo "   ./mvctest balance $MVC_ADDR"
echo ""
echo "3. Send transfer (requires balance first):"
echo "   ./mvctest send $PRIV_KEY <target_address> <amount(satoshi)>"
echo ""
echo "Example:"
echo "   ./mvctest send $PRIV_KEY 1BoatSLRHtKNngkdXEeobR76b53LETtpyT 100000"
echo ""

# Cleanup
rm keypair.txt

echo "Done!"
