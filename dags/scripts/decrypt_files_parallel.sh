#!/usr/bin/env bash

echo '{{ var.value.public_token }}' > /tmp/mypub.key
echo '{{ var.value.private_key }}' > /tmp/mypriv.key
gpg --batch --import /tmp/mypub.key
gpg --batch --import /tmp/mypriv.key

echo "listing Secret Keys"
gpg --list-keys
gpg --list-secret-keys

find {{ params.endpoint }} -name "*.gpg" | while read fldr; do
    echo $fldr
    file_name=$(basename $fldr)
    file_path=$(dirname $fldr)
    stripped_file_name=$(echo $file_name|sed  "s/.gpg//g")
    decrypted_folder=$(echo $file_path|sed  "s/encrypted/decrypted/g")
    extension="${stripped_file_name##*.}"
    echo "Decrypting File ${file_name} File Path:${file_path} -- Decrypted Folder & File - ${decrypted_folder}/${stripped_file_name}"
    mkdir -p ${decrypted_folder}
    gpg -v --pinentry-mode=loopback --batch --passphrase '{{ var.value.hdm_passphrase }}' -o ${decrypted_folder}/${stripped_file_name}  --decrypt ${fldr}
done

# Clean Up
rm -f /tmp/*.key

