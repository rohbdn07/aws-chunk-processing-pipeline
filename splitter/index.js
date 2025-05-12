const fs = require('fs');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { chain } = require('stream-chain');


const splitLargeJson = (inputFile, outputDir, chunkSize) => {
    return new Promise( (resolve, reject) => {
        const pipeline = chain([
            fs.createReadStream(inputFile),
            parser(),
            streamArray()
        ])
    
        let chunks = [];
        let chunkIndex = 1;
        let totalRecords = 0;
        const chunkFiles = []; 
    
        // Check for duplicates
        const uniqueKey = new Set();
        const duplicates = [];
    
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir)
        }
    
        pipeline.on('data', ({value}) => {
            const playerId = value.player_id;
    
             // Check for duplicate player_id
            if(uniqueKey.has(playerId)) {
                duplicates.push(value)
            } else {
                uniqueKey.add(playerId)
            }
            chunks.push(value)
            totalRecords++
    
             // If the chunks length reaches the given chunk size, write it to a file
            if(chunks.length === chunkSize) {
                const outputFile = `${outputDir}/chunk_${chunkIndex}.json`
                fs.writeFileSync(outputFile, JSON.stringify(chunks, null, 2))
                
                chunkFiles.push(`chunk_${chunkIndex}.json`);
                chunkIndex++
                // Calculate the memory size of the chunk
                const chunkSizeInBytes = Buffer.byteLength(JSON.stringify(chunks), 'utf8');
                const chunkSizeInMB = (chunkSizeInBytes / (1024 * 1024)).toFixed(2);                 
                console.log(`Created file: ${outputFile} with ${chunks.length} records. Chunk size: ${chunkSizeInMB} MB`);
                chunks = []
            }
        });
    
        pipeline.on('end', () => {
            if(chunks.length > 0) {
                const outputFile = `${outputDir}/chunk_${chunkIndex}.json`;
                fs.writeFileSync(outputFile, JSON.stringify(chunks, null, 2))
                // Calculate the memory size of the chunk
                const chunkSizeInBytes = Buffer.byteLength(JSON.stringify(chunks), 'utf8');
                const chunkSizeInMB = (chunkSizeInBytes / (1024 * 1024)).toFixed(2);
                console.log(`Created file: ${outputFile} with ${chunks.length} records. Chunk size: ${chunkSizeInMB} MB`);
                chunkFiles.push(`chunk_${chunkIndex}.json`); 
            }
            console.log(`Total records processed: ${totalRecords}`);
            console.log(`Total chunks created: ${chunkIndex}`);
    
            // Checks for duplicates
            if(duplicates.length > 0) {
                reject(
                    new Error(`Duplicate records detected in the input JSON file, Duplicate file: ${duplicates.length}`)
                );
                return;
            } else {
                console.log('No duplicate records found.');
            }
    
            // Verify no data loss at the end of the process
            const totalUniqueRecords = uniqueKey.size;
            if(totalRecords !== totalUniqueRecords) {
                reject(
                    new Error(`Data check failed: Total records (${totalRecords} do not match unique records (${totalUniqueRecords})`)
                );
                return;
            } else {
                console.log('Data check passed: NO records lost or duplicate')
            }

            // Resolve the promise with the list of chunk files
            resolve(chunkFiles);
    
        });
    
        pipeline.on('error', (err) => {
            console.error('Error while processing file', err)
        })

    })

}


const inputFile = '../data/mock-rud.json'
// const inputFile = '../data/moc-duplicate-rud.json';
const outputDir = '../data/chunks';
const chunkSize = 3000;

exports.handler = async () => {
    try {
        const keysToChunks = await splitLargeJson(inputFile, outputDir, chunkSize);
        console.log('Chunks created:', {
            keysToChunks: keysToChunks
        });
        return {
            keysToChunks: keysToChunks
        }; 
    } catch (error) {
        console.error('Error:', error.message);
    }
};

exports.handler();

/* splitLargeJson(inputFile, outputDir, chunkSize)
    .then((chunkFiles) => {
        console.log('Chunks created:', chunkFiles);
    })
    .catch((error) => {
        console.error('Error:', error.message);
    }); */

// Call the async function
// processChunks();



