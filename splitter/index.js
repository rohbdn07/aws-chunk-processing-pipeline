const fs = require('fs');
const { S3Client,  PutObjectCommand} = require('@aws-sdk/client-s3');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { chain } = require('stream-chain');
const path = require('path');



exports.handler = async () => {
    const inputFile = '../data/mock-rud.json'
    // const inputFile = '../data/moc-duplicate-rud.json';
    const outputDir = '../data/chunks';
    const chunkSize = 3000;

    try {
        const keysToChunks = await splitLargeJson(inputFile, outputDir, chunkSize);
        console.log('Chunks created:', {
            keysToChunks: keysToChunks
        });
        const filePath = path.join(outputDir, path.basename(key));
        for (const key of keysToChunks) {
            await uploadChunksDataToS3('report-store', key, filePath)
        }
        return {
            keysToChunks: keysToChunks
        }; 
    } catch (error) {
        console.error('Error:', error.message);
    }
};

const splitLargeJson = (inputFile, outputDir, chunkSize) => {
    const projectName = "projectA";
    const date = new Date().toISOString().split('T')[0]; 
    const dataType = "data/chunks";
    const s3Client = new S3Client();

    const generateKey = (chunkIndex) => {
        return `${projectName}/${date}/${dataType}/chunk_${chunkIndex}.json`;
    };

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
                chunkFiles.push(generateKey(chunkIndex));
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
                chunkFiles.push(generateKey(chunkIndex));
                
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
// Upload data chunks to S3 bucket
const uploadChunksDataToS3 = async(bucketName, key, filePath) => {
    try {
        const fileContent = fs.readFileSync(filePath);
        const s3details = {
            Bucket: bucketName,
            Key: key,
            Body: fileContent,
            ContentType: 'application/json'
    }
    await S3Client.send(new PutObjectCommand(s3details))
    console.log(`File uploaded to S3: ${key}`);
        
    } catch (error) {
        console.error(`Error uploading file to S3: ${key}`, error);
    }
    
    
}

exports.handler();





