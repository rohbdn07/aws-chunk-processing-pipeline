const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pipeline } = require('stream');
const { Writable } = require('stream');


/**
 * AWS Lambda handler function to process a large JSON file, split it into chunks,
 * and upload the chunks to an S3 bucket.
 *
 * Workflow:
 * 1. Reads a large JSON file from S3 bucket.
 * 2. Splits the JSON data into smaller chunks based on a specified chunk size.
 * 3. Generates unique S3 keys for each chunk.
 * 4. Uploads each chunk to the specified S3 bucket.
 * 5. Rollback (delete) uploaded chunks files if total chunks size is not equal to total original records.
 *
 * Environment Variables:
 * - BUCKET_NAME: The name of the S3 bucket where the chunks will be uploaded.
 * - AWS_REGION: The name of AWS region.
 * - LOCALSTACK_ENDPOINT: The name of localstack endpoint. Running LocalStack to emulate AWS services locally.
 *
 * @async
 * @function
 * @returns {Promise<Object>} An object containing the keys of the uploaded chunks.
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

        let chunkNumber = 1;
        let chunks = [];
        const keysToChunks = [];
        let uploadPromises = [];
        let totalRecords = 0;
        let chunkTotalRecords = [];


        const processBatchUploads = async () => {
            if (uploadPromises.length > 0) {
                await Promise.all(uploadPromises);
                uploadPromises = [];
            }
        }

        const writable = new Writable({
            objectMode: true,
            async write({ value }, _, callback) {
                try {
                    if (value) {
                        chunks.push(value);
                        totalRecords++

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

        // Total from multiple chunks records 
        const totalChunksRecords = chunkTotalRecords.reduce((accumulator, currentValue) => accumulator + currentValue, 0);

        // Verify no data loss at the end of the process
        if (totalRecords !== totalChunksRecords) {
            await rollBackUploadsFromS3(bucketName, keysToChunks);
            console.info(
                `ℹ️ Data integrity check failed (data lost): Original records (${totalRecords}) do not match written records (${totalChunksRecords}).\nℹ️ ROLLBACK: All chunk files have been deleted due to data integrity failure.`
            );
            throw new Error(`Data check failed: Total records (${totalRecords} do not match unique records (${totalChunksRecords})`);
        } else {
            console.info('ℹ️ Data check passed: No records lost');
        }

        console.info(`✅ Successfully uploaded ${chunkNumber} chunks files to S3.`);

        // Return S3 keys
        return { keysToChunks };
    } catch (error) {
        console.error("❌ Error processing file:", error);
        throw error;
    }
};


// Rollback uploaded chunks files from S3 bucket
async function rollBackUploadsFromS3(bucketName, fileKeys) {
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

const generateKey = (chunkNumber) => {
    const projectName = "projectA";
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const dataType = "data/monthly";
    return `${projectName}/${dataType}/${year}/${month}/chunk_${chunkNumber}.json`;
};


// Upload data chunks to S3 bucket
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


