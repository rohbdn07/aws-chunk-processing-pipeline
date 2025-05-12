const fs = require('fs');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { chain } = require('stream-chain');
const path = require('path');


/**
 * AWS Lambda handler function to process a large JSON file, split it into chunks,
 * and upload the chunks to an S3 bucket.
 *
 * Workflow:
 * 1. Reads a large JSON file from the local file system.
 * 2. Splits the JSON data into smaller chunks based on a specified chunk size.
 * 3. Generates unique S3 keys for each chunk.
 * 4. Uploads each chunk to the specified S3 bucket.
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
exports.handler = async () => {
    const inputFile = '../data/mock-rud.json'
    const outputDir = '../data/chunks';
    const chunkSize = 3000;
    const bucketName = process.env.BUCKET_NAME || 'report-store'

    try {
        const keysToChunks = await splitLargeJsonData(inputFile, outputDir, chunkSize);
        console.info('Chunks created:', {keysToChunks:keysToChunks});
        for (const key of keysToChunks) {
            const filePath = path.join(outputDir, path.basename(key));
            await uploadChunksDataToS3(bucketName, key, filePath)
        }
        return {keysToChunks: keysToChunks};
    } catch (error) {
        console.error('Error:', error.message);
    }
};

// Split  large Json data into chunks
const splitLargeJsonData = (inputFile, outputDir, chunkSize) => {
    const projectName = "projectA";
    const date = new Date().toISOString().split('T')[0];
    const dataType = "data/chunks";

    const generateKey = (chunkIndex) => {
        return `${projectName}/${date}/${dataType}/chunk_${chunkIndex}.json`;
    };

    return new Promise((resolve, reject) => {
        const pipeline = chain([
            fs.createReadStream(inputFile),
            parser(),
            streamArray()
        ])

        let chunks = [];
        let chunkIndex = 1;
        let totalRecords = 0;
        const keysToChunks = [];

        // Check for duplicates
        const uniqueKey = new Set();
        const duplicates = [];

        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir);
        }

        pipeline.on('data', ({ value }) => {
            const playerId = value.player_id;

            // Check for duplicate player_id
            if (uniqueKey.has(playerId)) {
                duplicates.push(value);
            } else {
                uniqueKey.add(playerId);
            }
            chunks.push(value);
            totalRecords++

            // If the chunks length reaches the given chunk size, write it to a file
            if (chunks.length === chunkSize) {
                const outputFile = `${outputDir}/chunk_${chunkIndex}.json`;
                fs.writeFileSync(outputFile, JSON.stringify(chunks, null, 2));
                keysToChunks.push(generateKey(chunkIndex));
                chunkIndex++
                console.info(`Created file: ${outputFile} with ${chunks.length} records.`);
                chunks = [];
            }
        });

        pipeline.on('end', () => {
            if (chunks.length > 0) {
                const outputFile = `${outputDir}/chunk_${chunkIndex}.json`;
                fs.writeFileSync(outputFile, JSON.stringify(chunks, null, 2));
                console.log(`Created file: ${outputFile} with ${chunks.length} records.`);
                keysToChunks.push(generateKey(chunkIndex));
            }
            console.info(`Total records processed: ${totalRecords}`);
            console.info(`Total chunks created: ${chunkIndex}`);

            // Checks for duplicates
            if (duplicates.length > 0) {
                reject(
                    new Error(`Duplicate records detected in the input JSON file. Duplicate file: ${duplicates.length}`)
                );
                return;
            } else {
                console.info('No duplicate records found.');
            }

            // Verify no data loss at the end of the process
            const totalUniqueRecords = uniqueKey.size;
            if (totalRecords !== totalUniqueRecords) {
                reject(
                    new Error(`Data check failed: Total records (${totalRecords} do not match unique records (${totalUniqueRecords})`)
                );
                return;
            } else {
                console.info('Data check passed: NO records lost');
            }

            // Resolve the promise with the list of chunk files
            resolve(keysToChunks);

        });

        pipeline.on('error', (err) => {
            console.error('Error while processing file', err);
        })

    })

}
// Upload data chunks to S3 bucket
const uploadChunksDataToS3 = async (bucketName, key, filePath) => {
    const s3Client = new S3Client({
        region: process.env.AWS_REGION || 'eu-west-1',
        endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localhost:4566',
        forcePathStyle: true,
    });
    try {
        const fileContent = fs.readFileSync(filePath);
        const s3details = {
            Bucket: bucketName,
            Key: key,
            Body: fileContent,
            ContentType: 'application/json'
        }
        await s3Client.send(new PutObjectCommand(s3details));
        console.info(`File uploaded to S3: ${key}`);

    } catch (error) {
        console.error(`Error uploading file to S3: ${key}`, error);
    }
}

exports.handler();





