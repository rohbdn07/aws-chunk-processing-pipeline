const fs = require('fs');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { pipeline, Writable } = require('stream');
const path = require('path');


// Constants
const INPUT_FILE = path.join(__dirname, '..', 'data', 'mock-rud.json')
const OUTPUT_DIR = path.join(__dirname, '..', 'data', 'chunks');
const CHUNK_SIZE = 3000;


/**
 * This function to process a large JSON file, split it into chunks,
 * and upload the chunks to output directory in the local file system.
 *
 * Workflow:
 * 1. Reads a large JSON file from the local file system.
 * 2. Splits the JSON data into smaller chunks based on a specified chunk size.
 * 3. Generates unique keys for each chunk.
 * 4. Uploads each chunk to the specified output directory.
 * 5. Check for data integrify (data lost), if found --> delete all chunks files from file system.
 *
 * @async
 * @function
 * @returns {Promise<Object>} An object containing the keys of the uploaded chunks.
 * @throws {Error} If an error occurs during processing or uploading.
 */
const primaryTaskSplitLargeJsonIntoChunks = async () => {
    try {
        if (!fs.existsSync(OUTPUT_DIR)) {
            fs.mkdirSync(OUTPUT_DIR);
        }
        const { totalOriginalRecords, totalChunksRecords, keysToChunks } = await splitLargeJsonData(INPUT_FILE, OUTPUT_DIR, CHUNK_SIZE);

        // Verify data integrity
        if (totalOriginalRecords === totalChunksRecords) {
            console.info(`✅ Data integrity check passed: ${totalOriginalRecords} records processed and written.`);
        } else {
            console.error(
                `❌ Data integrity check failed (data lost): Original records (${totalOriginalRecords}) do not match written records (${totalChunksRecords}).`
            );

            // Delete all chunk files if data integrity fails
            const files = fs.readdirSync(OUTPUT_DIR).filter((file) => file.startsWith('projectA-'));
            for (const file of files) {
                fs.unlinkSync(path.join(OUTPUT_DIR, file));
            }
            console.error('❌ All chunk files have been deleted due to data integrity failure.');
        }
        return { keysToChunks };
    } catch (error) {
        console.error('Error:', error.message);
        throw new Error(`Error: split large json data into multiple chunks files failed`);
    }
}

/**
 * Writes a chunk of JSON data to a file with a generated unique name in the specified output directory.
 *
 * @param {string} outputDir - Directory where the chunk file will be written.
 * @param {number} chunkNumber - The chunk index (used for file naming).
 * @param {Array} data - The array of records to write.
 * @returns {Promise<{fileName: string, writtenRecordsCount: number}>} The file name and number of records written.
 * @throws {Error} If writing to the file fails.
 */
const writeChunkToFile = async (outputDir, chunkNumber, data) => {
    const fileName = generateKey(chunkNumber);
    const outputFilePath = path.join(outputDir, fileName);
    let writtenRecordsCount = 0;

    try {
        await fs.promises.writeFile(outputFilePath, JSON.stringify(data, null, 2));
        writtenRecordsCount += data.length;
        console.info(`✅ Created file: ${outputFilePath} with ${data.length} records.`);
        return { fileName, writtenRecordsCount };
    } catch (error) {
        console.error(`❌ Error writing to file ${fileName}:`, error.message);
        throw error;
    }
};


/**
 * Reads a large JSON array from a file as a stream, splits it into smaller chunks,
 * and writes each chunk to a separate file in the specified output directory.
 * Tracks the total number of records processed and ensures all data is chunked and written.
 *
 * @param {string} data - Path to the input JSON file.
 * @param {string} outputDir - Directory where chunk files will be written.
 * @param {number} chunkSize - Number of records per chunk file.
 * @returns {Promise<{totalOriginalRecords: number, totalChunksRecords: number, keysToChunks: string[]}>}
 *          An object containing the total records processed, total records written, and the list of chunk file names.
 * @throws {Error} If an error occurs during reading, chunking, or writing.
 */
const splitLargeJsonData = async (data, outputDir, chunkSize) => {
    let chunks = [];
    let totalOriginalRecords = 0;
    const keysToChunks = [];
    let chunkNumber = 1;
    let chunkTotalRecords = [];

    const writable = new Writable({
        objectMode: true,
        async write({ value }, _, callback) {
            try {
                if (value) {
                    chunks.push(value);
                    totalOriginalRecords++;

                    // When chunk size reached, write to file
                    if (chunks.length == chunkSize) {
                        const { fileName, writtenRecordsCount } = await writeChunkToFile(outputDir, chunkNumber, chunks);
                        keysToChunks.push(fileName);
                        chunkTotalRecords.push(writtenRecordsCount)
                        chunks = [];
                        chunkNumber++;
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
                // Write remaining records to a final chunk
                if (chunks.length > 0) {
                    const { fileName, writtenRecordsCount } = await writeChunkToFile(outputDir, chunkNumber, chunks);
                    console.log(`✅ Splitting complete. Total records processed: ${totalOriginalRecords}`);
                    keysToChunks.push(fileName);
                    chunkTotalRecords.push(writtenRecordsCount);
                    callback();
                }
            } catch (error) {
                console.error("Error in uploading final chunk:", error);
                callback(error);
            }
        }
    })

    await new Promise((resolve, reject) => {
        pipeline(
            fs.createReadStream(data),
            parser(),
            streamArray(),
            writable,
            (error) => {
                if (error) {
                    console.error("❌ Error while processing stream:", error);
                    reject(error);
                } else {
                    console.info(`✅ Successfully processed ${totalOriginalRecords} records into ${chunkNumber} files.`);
                    resolve()
                }
            }
        );
    });

    const totalChunksRecords = chunkTotalRecords.reduce((accumulator, currentValue) => accumulator + currentValue, 0);
    return { totalOriginalRecords, totalChunksRecords, keysToChunks };
}


/**
 * Generates a unique chunk file name based on the project, data type, current year and month, and chunk number.
 *
 * @param {number} chunkNumber - The chunk index (starting from 1).
 * @returns {string} The generated file name, e.g., "projectA-data-monthly-2024-06-chunk_1.json".
 */
const generateKey = (chunkNumber) => {
    const projectName = "projectA";
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const dataType = "data-monthly";
    return `${projectName}-${dataType}-${year}-${month}-chunk_${chunkNumber}.json`;
};

module.exports = { primaryTaskSplitLargeJsonIntoChunks, splitLargeJsonData, writeChunkToFile, generateKey};



/**
 * NOTE TO DEVELOPERS:
 * To run this script directly, uncomment the function call below.
 * This will execute the primaryTaskSplitLargeJsonIntoChunks function.
 */
// primaryTaskSplitLargeJsonIntoChunks()