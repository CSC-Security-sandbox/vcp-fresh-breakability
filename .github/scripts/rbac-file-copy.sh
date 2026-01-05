#!/bin/bash
#set -x

source_bucket=$1
rbac_file_path=$2
rbac_file_name=$3
release_version=$4

#check files exist in the source bucket
gsutil -q stat "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}"
RBAC_FILE_EXIST=$?
if [ ${RBAC_FILE_EXIST} -ne 0 ]; then
    echo "❌ Error: RBAC file not found in the source bucket"
    exit 1
fi

gsutil -q stat "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}.md5"
MD5_FILE_EXIST=$?
if [ ${MD5_FILE_EXIST} -ne 0 ]; then
    echo "❌ Error: RBAC MD5 file not found in the source bucket"
    exit 1
fi
echo "✅ Files found in the source bucket"




#validate checksum of the files by validating the md5 file of rbac file
gsutil cp "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}" .
gsutil cp "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}.md5" .
checksum=$(md5sum "${rbac_file_name}" | awk '{print $1}')
checksum_md5=$(cat "${rbac_file_name}.md5" | awk '{print $1}')

if [ "$checksum" != "$checksum_md5" ]; then
    echo "❌ Error: Checksum mismatch for the file"
    exit 1
else 
    echo "✅ Checksum match for the file"
fi


# Copy RBAC file from source bucket to destination bucket
gsutil cp "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}" "gs://vsa-compute-images/GCNV/${release_version}/RBAC/${rbac_file_name}"
#validate file copied successfully to gcp storage bucket vsa-compute-images
gsutil -q stat "gs://vsa-compute-images/GCNV/${release_version}/RBAC/${rbac_file_name}"
RBAC_FILE_EXIST=$?
if [ ${RBAC_FILE_EXIST} -ne 0 ]; then
    echo "❌ Error: RBAC file not found in the gcp storage bucket vsa-compute-images"
    exit 1
fi
echo "✅ Files found in the source bucket"


# Copy RBAC MD5 file from source bucket to destination bucket
gsutil cp "gs://${source_bucket}/${rbac_file_path}/${rbac_file_name}.md5" "gs://vsa-compute-images/GCNV/${release_version}/RBAC/${rbac_file_name}.md5"
gsutil -q stat "gs://vsa-compute-images/GCNV/${release_version}/RBAC/${rbac_file_name}.md5"
MD5_FILE_EXIST=$?
if [ ${MD5_FILE_EXIST} -ne 0 ]; then
    echo "❌ Error: RBAC MD5 file not found in the gcp storage bucket vsa-compute-images"
    exit 1
fi


echo "✅ RBAC file and MD5 file copied to destination bucket vsa-compute-images"
