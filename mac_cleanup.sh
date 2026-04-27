#!/bin/bash
# mac-cleanup.sh — interactive cleanup for bloated System Data
# Run with: bash mac-cleanup.sh

set -e

confirm() {
    read -p "$1 [y/N]: " response
    [[ "$response" =~ ^[Yy]$ ]]
}

size() {
    du -sh "$1" 2>/dev/null | cut -f1
}

echo "=== Mac Cleanup Script ==="
echo "This will show sizes and ask before deleting anything."
echo

# 1. Time Machine local snapshots
echo "--- Time Machine local snapshots ---"
snapshots=$(tmutil listlocalsnapshots / 2>/dev/null | grep -o 'com.apple.TimeMachine[^ ]*' || true)
if [[ -n "$snapshots" ]]; then
    echo "$snapshots"
    if confirm "Delete all local Time Machine snapshots?"; then
        echo "$snapshots" | while read snap; do
            date=$(echo "$snap" | sed 's/com.apple.TimeMachine.//; s/.local//')
            sudo tmutil deletelocalsnapshots "$date" || true
        done
    fi
else
    echo "None found."
fi
echo

# 2. User caches
echo "--- User caches (~/Library/Caches) ---"
echo "Current size: $(size ~/Library/Caches)"
if confirm "Clear user caches? (Safe — apps will rebuild them)"; then
    rm -rf ~/Library/Caches/* 2>/dev/null || true
    echo "Cleared."
fi
echo

# 3. Xcode junk (only if Xcode is installed)
if [[ -d ~/Library/Developer ]]; then
    echo "--- Xcode DerivedData ---"
    echo "Size: $(size ~/Library/Developer/Xcode/DerivedData)"
    if confirm "Delete Xcode DerivedData?"; then
        rm -rf ~/Library/Developer/Xcode/DerivedData/* 2>/dev/null || true
    fi

    echo "--- iOS Device Support (old iOS versions) ---"
    echo "Size: $(size ~/Library/Developer/Xcode/iOS\ DeviceSupport)"
    if confirm "Delete iOS DeviceSupport? (Xcode redownloads if needed)"; then
        rm -rf ~/Library/Developer/Xcode/iOS\ DeviceSupport/* 2>/dev/null || true
    fi

    echo "--- CoreSimulator caches ---"
    echo "Size: $(size ~/Library/Developer/CoreSimulator/Caches)"
    if confirm "Delete simulator caches?"; then
        rm -rf ~/Library/Developer/CoreSimulator/Caches/* 2>/dev/null || true
    fi
fi
echo

# 4. iOS backups
if [[ -d ~/Library/Application\ Support/MobileSync/Backup ]]; then
    echo "--- iOS device backups ---"
    ls -lh ~/Library/Application\ Support/MobileSync/Backup/ 2>/dev/null || true
    echo "Size: $(size ~/Library/Application\ Support/MobileSync/Backup)"
    echo "(NOT auto-deleting — review manually in Finder if you want to remove old ones)"
fi
echo

# 5. Logs
echo "--- Logs ---"
echo "Size: $(size ~/Library/Logs)"
if confirm "Clear user logs?"; then
    rm -rf ~/Library/Logs/* 2>/dev/null || true
fi
echo

# 6. Downloads folder reminder
echo "--- Downloads folder ---"
echo "Size: $(size ~/Downloads)"
echo "(Not touching this — review manually)"
echo

# 7. Trash
echo "--- Trash ---"
echo "Size: $(size ~/.Trash)"
if confirm "Empty Trash?"; then
    rm -rf ~/.Trash/* 2>/dev/null || true
fi
echo

# 8. Purge memory (forces macOS to recalculate storage)
if confirm "Run 'sudo purge' to flush memory and refresh storage stats?"; then
    sudo purge
fi

echo
echo "=== Done. Check Storage in System Settings to see the new numbers. ==="
