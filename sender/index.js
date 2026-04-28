const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const JSONStream = require('JSONStream');


/**
 * AWS Lambda handler that validates a single report chunk created by the splitter.
 *
 * The Step Function Map state invokes this handler once per chunk key. For each chunk,
 * the handler downloads the JSON array from S3, verifies that it contains between 1 and
 * 3000 player records, and checks that every record has a player_id.
 *
 * @param {string} event - S3 object key for a single report chunk.
 * @returns {Promise<{validatedReportChunkSizes: Array<{key: string, size: number}>}>}
 */
exports.handler = async (event) => {
    const key = validateInputEvent(event);
    const bucketName = process.env.BUCKET_NAME || 'report-store';
    const validatedReportChunkSizes = [];

    console.info(`Received report chunk key: ${key}`);

    try {
        const params = {
            Bucket: bucketName,
            Key: key,
            ResponseContentType: 'application/json',
        };
        const resp = await s3Client.send(new GetObjectCommand(params));
        console.info(`Successfully retrieved data from S3 for key: ${key}`);

        const chunkSize = await countValidPlayers(resp.Body);
        if (chunkSize === 0) {
            throw new Error(`Invalid data retrieved from S3 for key: ${key} - no player records found`);
        }
        if (chunkSize > 3000) {
            throw new Error(`Invalid data retrieved from S3 for key: ${key}. Expected up to 3000 records, got ${chunkSize}`);
        }

        validatedReportChunkSizes.push({
            key,
            size: chunkSize,
        });
    } catch (error) {
        console.error(`Failed to validate data from S3 for key: ${key}`, error);
        throw error;
    }

    return { validatedReportChunkSizes };
};

const s3Client = new S3Client({
    region: process.env.AWS_REGION || 'eu-west-1',
    endpoint: process.env.LOCALSTACK_ENDPOINT || 'http://localstack_compliance_tech_recruitment_assignment:4566',
    forcePathStyle: true,
});

/**
 * Validates that the sender received a single chunk key from the Step Function Map state.
 *
 * @param {string} event - S3 object key for a single report chunk.
 * @returns {string} The validated chunk key.
 */
const validateInputEvent = (event) => {
    if (!event || typeof event !== 'string') {
        throw new Error('Invalid input event. Sender expects a single S3 chunk key string');
    }

    return event;
};

const countValidPlayers = async (stream) => {
    let count = 0;
    return new Promise((resolve, reject) => {
        stream
            .pipe(JSONStream.parse('*'))
            .on('data', (player) => {
                if (!player?.player_id) {
                    reject(new Error('Invalid chunk record found without player_id'));
                    return;
                }
                count++;
            })
            .on('end', () => resolve(count))
            .on('error', reject);
    });
};
