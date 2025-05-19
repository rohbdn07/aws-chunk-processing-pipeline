const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pipeline } = require('stream');
const { Writable } = require('stream');


/**
 * AWS Lambda handler function to process a large JSON file from S3, split it into chunks,
 * and upload the chunks back to S3. Ensures data integrity by rolling back uploads if any data loss is detected.
 *
 * NOTE: For reliable processing of large files, increase the Lambda function timeout from the default (3 seconds)
 * to at least 8 seconds, or higher if needed, to avoid premature termination.
 *
 * Workflow:
 * 1. Reads a large JSON file from an S3 bucket.
 * 2. Streams and splits the JSON array into smaller chunks based on a specified chunk size.
 * 3. Generates unique S3 keys for each chunk.
 * 4. Uploads each chunk to the specified S3 bucket, batching uploads for efficiency.
 * 5. If the total number of records uploaded does not match the original, deletes all uploaded chunks (rollback).
 *
 * Environment Variables:
 * - BUCKET_NAME: The name of the S3 bucket where the chunks will be uploaded.
 * - AWS_REGION: The AWS region.
 * - LOCALSTACK_ENDPOINT: The LocalStack endpoint for local AWS emulation.
 *
 * @param {Object} event - Lambda event object containing at least the S3 key and optionally the bucket name.
 * @param {string} event.key - The S3 key of the input JSON file.
 * @param {string} [event.bucketName] - The S3 bucket name.
 * @returns {Promise<{keysToChunks: string[]}>} An object containing the keys of the uploaded chunks.
 * @throws {Error} If an error occurs during processing or uploading.
 */
exports.handler = async (event) => {
    const CHUNK_SIZE = 3000;
    const BATCH_SIZE = 2;
    const key = event.key;
    const bucketName = process.env.BUCKET_NAME || event.bucketName || 'report-store'

    try {
        if (!bucketName || !key) {
            throw new Error('Missing required parameters: bucketName or key');
        }
        const params = {
            Bucket: bucketName,
            Key: key,
            ResponseContentType: 'application/json',
        };
        const s3Client = new S3Client();
        const resp = await s3Client.send(new GetObjectCommand(params));
        const largeJsonData = resp.Body;
        console.info(`Successfully retrieved data from S3 for key: ${key}`);

        const keysToChunks = [];
        let chunkNumber = 1;
        let chunks = [];
        let uploadPromises = [];
        let totalOriginalRecords = 0;
        let chunkTotalRecords = [];

        const processBatchUploads = async () => {
            if (uploadPromises.length > 0) {
                await Promise.all(uploadPromises);
                uploadPromises = [];
            }
        }

        // Writable stream to collect, batch, and upload JSON chunks to S3.
        const writable = new Writable({
            objectMode: true,
            async write({ value }, _, callback) {
                try {
                    if (value) {
                        chunks.push(value);
                        totalOriginalRecords++

                        // Upload when chunk size is reached
                        if (chunks.length == CHUNK_SIZE) {
                            let chunkKey = generateKey(chunkNumber)
                            keysToChunks.push(chunkKey);
                            const uploadPromise = uploadChunksDataToS3(bucketName, chunkKey, chunks);
                            uploadPromises.push(uploadPromise);
                            chunkTotalRecords.push(chunks.length);
                            chunks = [];
                            chunkNumber++;

                            // When batch size is reached, upload batch
                            if (uploadPromises.length >= BATCH_SIZE) {
                                await processBatchUploads()
                            }
                        }
                    }
                    callback();
                } catch (error) {
                    console.error("Error writing stream:", error)
                    callback(error);
                }
            },

            async final(callback) {
                try {
                    // Upload any remaining data
                    if (chunks.length > 0) {
                        const chunkKey = generateKey(chunkNumber);
                        keysToChunks.push(chunkKey);
                        uploadPromises.push(uploadChunksDataToS3(bucketName, chunkKey, chunks));
                        chunkTotalRecords.push(chunks.length);
                    }
                    await processBatchUploads();
                    console.info(`ℹ️ Final chunk uploaded. Total chunks: ${chunkNumber}`);
                    callback();
                } catch (error) {
                    console.error("Error in uploading final chunk:", error);
                    callback(error);
                }
            }
        });

        // Stream pipeline to process JSON in chunks
        await new Promise((resolve, reject) => {
            pipeline(
                largeJsonData,
                parser(),
                streamArray(),
                writable,
                (err) => (err ? reject(err) : resolve())
            );
        });

        // Total chunks from multiple chunks records 
        const totalChunksRecords = chunkTotalRecords.reduce((accumulator, currentValue) => accumulator + currentValue, 0);

        // Verify no data loss at the end of the process
        if (totalOriginalRecords !== totalChunksRecords) {
            await rollBackUploadsFromS3(bucketName, keysToChunks);
            console.info(
                `ℹ️ Data integrity check failed (data lost): Original records (${totalOriginalRecords}) do not match written records (${totalChunksRecords}).\nℹ️ ROLLBACK: All chunk files have been deleted due to data integrity failure.`
            );
            throw new Error(`Data check failed: Total records (${totalOriginalRecords} do not match unique records (${totalChunksRecords})`);
        } else {
            console.info('ℹ️ Data check passed: No records lost');
        }
        console.info(`✅ Successfully uploaded ${chunkNumber} chunks files to S3.`);

        return { keysToChunks };

    } catch (error) {
        console.error("❌ Error processing file:", error);
        throw error;
    }
};


// Rollback uploaded chunks from S3 bucket
const rollBackUploadsFromS3 = async (bucketName, fileKeys) => {
    const s3Client = new S3Client({
        region: process.env.AWS_REGION || 'eu-west-1',
        endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_compliance_tech_recruitment_assignment:4566',
        forcePathStyle: true,
    });

    try {
        const deleteParams = {
            Bucket: bucketName,
            Delete: {
                Objects: fileKeys?.map((key) => ({ Key: key }))
            }
        };
        const result = await s3Client.send(new DeleteObjectsCommand(deleteParams));
        console.log(`📂 Deleted files from S3 bucket: ${result.Deleted.map(f => f.Key).join(', ')}`);
    } catch (error) {
        console.error("Error deleting chunk files:", error);
    }
}

// Generate chunk file name
const generateKey = (chunkNumber) => {
    const projectName = "projectA";
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const dataType = "data/monthly";
    return `${projectName}/${dataType}/${year}/${month}/chunk_${chunkNumber}.json`;
};


/**
 * Uploads a chunk of JSON data to the specified S3 bucket and key.
 *
 * @param {string} bucketName - The name of the S3 bucket.
 * @param {string} key - The S3 object key for the chunk file.
 * @param {Array} data - The chunk of data to upload.
 * @returns {Promise<void>}
 * @throws {Error} If the upload to S3 fails.
 */
const uploadChunksDataToS3 = async (bucketName, key, data) => {
    const s3Client = new S3Client({
        region: process.env.AWS_REGION || 'eu-west-1',
        endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_compliance_tech_recruitment_assignment:4566',
        forcePathStyle: true,
    });
    try {
        const s3details = {
            Bucket: bucketName,
            Key: key,
            Body: JSON.stringify(data, null, 2),
            ContentType: 'application/json'
        }
        await s3Client.send(new PutObjectCommand(s3details));
        console.info(`ℹ️ File uploaded to S3: ${key}`);
    } catch (error) {
        console.error(`❌ Error: upload file to S3: ${key}`, error);
        throw error;
    }
}


