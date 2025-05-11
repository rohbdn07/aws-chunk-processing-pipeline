const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const JSONStream = require('JSONStream');

exports.handler = async (event) => {
    console.info(`Received splitter output: ${JSON.stringify(event)}`);
    validateInputEvent(event);
    const s3Client = new S3Client();
    const bucketName = process.env.BUCKET_NAME;
    const validatedReportChunkSizes = [];
    for (const key of event.keysToChunks) {
        try {
            const params = {
                Bucket: bucketName,
                Key: key,
                ResponseContentType: 'application/json',
            };
            const resp = await s3Client.send(new GetObjectCommand(params));
            console.info(`Successfully retrieved data from S3 for key: ${key}`);
            const chunkSize = await countPlayers(resp.Body);
            if (!chunkSize) {
                throw new Error(`Invalid data retrieved from S3 for key: ${key} - no player records found`);
            }
            if (chunkSize > 3000) {
                throw new Error(`Invalid data retrieved from S3 for key: ${key}. Expected up to 3000 records, got ${reportChunk.length}`);
            }
            validatedReportChunkSizes.push({
                key,
                size: chunkSize,
            });
        } catch (error) {
            console.error(`Failed to validate data from S3 for key: ${key}`, error);
            throw error;
        }
    }
    return { validatedReportChunkSizes };
};

const validateInputEvent = (event) => {
    if (!event || !event.keysToChunks || !event.keysToChunks.length) {
        throw new Error('Invalid input event. Splitter output must contain keysToChunks array');
    }
};

const countPlayers = async (stream) => {
    let nbPlayers = 0;
    return new Promise((resolve, reject) => {
        stream
            .pipe(JSONStream.parse('*.player_id'))
            .on('data', (playerId) => {
                if (playerId) {
                    nbPlayers++;
                }
            })
            .on('end', () => resolve(nbPlayers))
            .on('error', (err) => {
                reject(err);
            });
    });
};
