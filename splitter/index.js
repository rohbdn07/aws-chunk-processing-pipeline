const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const pLimit = require('p-limit');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pipeline } = require('stream');
const { Writable } = require('stream');



const s3Client = new S3Client({
    region: process.env.AWS_REGION || 'eu-west-1',
    endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_data_pipeline:4566',
    forcePathStyle: true,
});

const snsClient = new SNSClient({
    region: process.env.AWS_REGION || 'eu-west-1',
    endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_data_pipeline:4566',
});

const sqsClient = new SQSClient({
    region: process.env.AWS_REGION || 'eu-west-1',
    endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_data_pipeline:4566',
});


/**
 * AWS Lambda handler function to process a large JSON file from S3, split it into chunks,
 * and upload the chunks back to S3. Ensures data integrity by rolling back uploads if any data loss is detected.
 *
 * NOTE: For reliable processing of large files, increase the Lambda function timeout from the default (3 seconds)
 * to at least 100 seconds, or higher if needed, to avoid premature termination. Also, ensure that you run lambda services in same region (eu-west-1).
 *
 * Workflow:
 * 1. Reads a large JSON file from an S3 bucket.
 * 2. Streams and splits the JSON array into smaller chunks based on a specified chunk size.
 * 3. Generates unique S3 keys for each chunk.
 * 4. Uploads each chunk to the specified S3 bucket with bounded concurrency.
 * 5. Sends an SQS message for each successfully uploaded chunk.
 * 6. If processing fails or uploaded record counts do not match the original, deletes uploaded chunks (rollback).
 *
 * Environment Variables:
 * - BUCKET_NAME: The name of the S3 bucket where the chunks will be uploaded.
 * - AWS_REGION: The AWS region.
 * - LOCALSTACK_ENDPOINT: The LocalStack endpoint for local AWS emulation.
 * - SQS_URL: The SQS queue URL that receives uploaded chunk messages.
 * - SNS_TOPIC_ARN: The SNS topic ARN for completion notifications.
 *
 * @param {Object} event - Lambda event object containing at least the S3 key and optionally the bucket name.
 * @param {string} event.key - The S3 key of the input JSON file.
 * @param {string} [event.bucketName] - The S3 bucket name.
 * @param {string} [event.bucket] - Alternative S3 bucket name field used by the Step Function input.
 * @returns {Promise<{keysToChunks: string[]}>} An object containing the keys of the uploaded chunks.
 * @throws {Error} If an error occurs during processing or uploading.
 */
exports.handler = async (event) => {
    const CHUNK_SIZE = 3000;
    const CONCURRENCY_LIMIT = 5;
    const limit = pLimit(CONCURRENCY_LIMIT);
    const keysToChunks = [];
    const uploadedChunkKeys = [];
    let bucketName;

    try {
        const key = event?.key;
        bucketName = event?.bucketName || event?.bucket || process.env.BUCKET_NAME || 'report-store'

        if (!bucketName || !key) {
            throw new Error('Missing required parameters: bucketName or key');
        }
        const chunkKeyPrefix = generateChunkKeyPrefix(key);
        const params = {
            Bucket: bucketName,
            Key: key,
            ResponseContentType: 'application/json',
        };
        const resp = await s3Client.send(new GetObjectCommand(params));
        const largeJsonData = resp.Body;
        console.info(`Successfully retrieved data from S3 for key: ${key}`);

        let chunkNumber = 1;
        let chunks = [];
        let uploadPromises = [];
        let validRecordCount = 0;
        let uploadedRecordCount = 0;
        let invalidRecordCount = 0;
        let duplicateRecordCount = 0;
        const seenPlayerIds = new Set();

        // Writable stream to collect and upload JSON chunks to S3.
        const writable = new Writable({
            objectMode: true,
            async write({ value }, _, callback) {
                try {
                    if (value) {
                        if (!value.player_id) {
                            invalidRecordCount++;
                            return callback();
                        }

                        if (seenPlayerIds.has(value.player_id)) {
                            duplicateRecordCount++;
                            return callback();
                        }

                        seenPlayerIds.add(value.player_id);
                        chunks.push(value);
                        validRecordCount++

                        if (chunks.length == CHUNK_SIZE) {
                            const chunkData = [...chunks];
                            let chunkKey = generateKey(chunkKeyPrefix, chunkNumber);
                            keysToChunks.push(chunkKey);
                            const uploadPromise = limit(async () => {
                                await uploadChunksDataToS3(bucketName, chunkKey, chunkData);
                                uploadedChunkKeys.push(chunkKey);
                                uploadedRecordCount += chunkData.length;
                                console.info(`✅ Uploaded to S3: ${chunkKey}`);
                                await sendSQS(bucketName, chunkKey);
                            });

                            uploadPromises.push(uploadPromise);
                            chunks = [];
                            chunkNumber++;
                        }
                    }
                    callback();
                } catch (error) {
                    callback(error);
                }
            },

            async final(callback) {
                try {
                    if (chunks.length > 0) {
                        const chunkData = [...chunks];
                        const chunkKey = generateKey(chunkKeyPrefix, chunkNumber);
                        keysToChunks.push(chunkKey);

                        const uploadPromise = limit(async () => {
                            await uploadChunksDataToS3(bucketName, chunkKey, chunkData);
                            uploadedChunkKeys.push(chunkKey);
                            uploadedRecordCount += chunkData.length;
                            console.info(`✅ Uploaded (final) to S3: ${chunkKey}`);
                            await sendSQS(bucketName, chunkKey);
                        });
                        uploadPromises.push(uploadPromise);
                    }
                    // Wait for all uploads to settle before deciding whether rollback is needed.
                    const uploadResults = await Promise.allSettled(uploadPromises);
                    const failedUploads = uploadResults.filter(({ status }) => status === 'rejected');
                    if (failedUploads.length > 0) {
                        throw new Error(`${failedUploads.length} chunk upload task(s) failed`);
                    }
                    uploadPromises.length = 0;
                    console.info(`✅ All chunks uploaded. Total chunks: ${keysToChunks.length}`);
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

        await verifyUploadIntegrity({
            bucketName,
            uploadedChunkKeys,
            validRecordCount,
            uploadedRecordCount,
        });

        console.info(`✅ Successfully uploaded ${keysToChunks.length} chunks files to S3.`);

        await sendNotification(keysToChunks.length);

        return {
            keysToChunks,
            validRecordCount,
            invalidRecordCount,
            duplicateRecordCount,
        };
    } catch (error) {
        console.error("❌ Error processing file:", error);
        try {
            await rollBackUploadsFromS3(bucketName, uploadedChunkKeys);
        } catch (rollbackError) {
            console.error("❌ Rollback failed after processing error:", rollbackError);
        }
        throw new Error(`❌ Error processing file:, ${error}`);
    }
};


// Send notification (message) to another server 
const sendNotification = async (chunkNumber) => {
    try {
        await snsClient.send(
            new PublishCommand({
                TopicArn: process.env.SNS_TOPIC_ARN || "arn:aws:sns:eu-west-1:000000000000:splitter_status",
                Message: `Your task is complete! ✅ Successfully uploaded ${chunkNumber} chunks files to S3.`,
            })
        )
        console.log('✅ Notification sent');
    } catch (error) {
        console.error("❌ Error sending Notification:", error);
    }
}

// Send SQS message
const sendSQS = async (bucketName, key) => {
    try {
        const response = await sqsClient.send(
            new SendMessageCommand({
                QueueUrl: process.env.SQS_URL,
                MessageBody: JSON.stringify({ bucket: bucketName, key })
            })
        )
        console.info('✅ SQS message sent:', {
            MessageId: response.MessageId,
            bucketName,
            key,
        });
    } catch (error) {
        console.error('❌ Failed SQS', error)
        throw new Error(`❌ Failed SQS:, ${error}`);
    }
}


const verifyUploadIntegrity = async ({ bucketName, uploadedChunkKeys, validRecordCount, uploadedRecordCount }) => {
    if (validRecordCount !== uploadedRecordCount) {
        await rollBackUploadsFromS3(bucketName, uploadedChunkKeys);
        uploadedChunkKeys.length = 0;
        console.info(
            `ℹ️ Data integrity check failed (data lost): Valid records (${validRecordCount}) do not match uploaded records (${uploadedRecordCount}).\nℹ️ ROLLBACK: All chunk files have been deleted due to data integrity failure.`
        );
        throw new Error(`Data check failed: Valid records (${validRecordCount}) do not match uploaded records (${uploadedRecordCount})`);
    }

    console.info('ℹ️ Data check passed: No valid records lost');
}


/**
 * Deletes a list of chunk files from the specified S3 bucket (rollback) if data integrity fails.
 *
 * @param {string} bucketName - The name of the S3 bucket.
 * @param {string[]} fileKeys - Array of S3 object keys to delete.
 * @returns {Promise<void>}
 * @throws {Error} If the deletion from S3 fails.
 */
const rollBackUploadsFromS3 = async (bucketName, fileKeys) => {
    try {
        if (!fileKeys || fileKeys.length === 0) {
            console.info('No chunk files to delete during rollback');
            return;
        }

        const deleteParams = {
            Bucket: bucketName,
            Delete: {
                Objects: fileKeys.map((key) => ({ Key: key }))
            }
        };
        const result = await s3Client.send(new DeleteObjectsCommand(deleteParams));
        console.log(`📂 Deleted files from S3 bucket: ${result.Deleted.map(f => f.Key).join(', ')}`);
    } catch (error) {
        console.error("Error deleting chunk files:", error);
        throw error;
    }
}

/**
 * Generates a unique S3 chunk key prefix based on project name, data type, current year/month, source file name, and run timestamp.
 * The format is: "projectA/data/monthly/YYYY/MM/{sourceFileName}_{runId}"
 *
 * @param {string} sourceKey - The S3 object key of the original JSON file.
 * @returns {string} The generated S3 object key prefix for this splitter run.
 */
const generateChunkKeyPrefix = (sourceKey) => {
    const projectName = "projectA";
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const dataType = "data/monthly";
    const sourceFileName = sourceKey.split('/').pop().replace(/\.json$/i, '');
    const runId = date.toISOString().replace(/[-:.TZ]/g, '');
    return `${projectName}/${dataType}/${year}/${month}/${sourceFileName}_${runId}`;
};

const generateKey = (chunkKeyPrefix, chunkNumber) => {
    return `${chunkKeyPrefix}/chunk_${chunkNumber}.json`;
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
    const s3details = {
        Bucket: bucketName,
        Key: key,
        Body: JSON.stringify(data, null, 2),
        ContentType: 'application/json'
    };

    return s3Client.send(new PutObjectCommand(s3details));
};
