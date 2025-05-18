// Import dependencies
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'data', 'chunks');

// Mock the required functions
jest.mock('fs', () => ({
    promises: {
        writeFile: jest.fn(),
    },
    existsSync: jest.fn(),
    mkdirSync: jest.fn(),
    readdirSync: jest.fn(),
    unlinkSync: jest.fn(),
    createReadStream: jest.fn()
}));

// Mock console methods to avoid real logs
console.info = jest.fn();
console.error = jest.fn();

// Import the functions to test
const { primaryTaskSplitLargeJsonIntoChunks, writeChunkToFile, generateKey } = require('./primaryTaskSplitLargeJson');

// Mock generateKey to return predictable filenames
const mockGenerateKey = (chunkNumber) => `projectA-data-monthly-2025-05-chunk_${chunkNumber}.json`;


// Test writeChunkToFile function
describe('writeChunkToFile', () => {
    const outputDir = `${OUTPUT_DIR}`;
    const chunkNumber = 1;
    const data = [{ id: 1 }, { id: 2 }];
    const fileName = mockGenerateKey(chunkNumber);
    const outputFilePath = path.join(outputDir, fileName);

    beforeEach(() => {
        jest.clearAllMocks();
        jest.spyOn(require('./primaryTaskSplitLargeJson'), 'writeChunkToFile').mockImplementation(
            async (outputDir, chunkNumber, data) => {
                const fileName = mockGenerateKey(chunkNumber);
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
            }
        );
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    test('should write data to file successfully', async () => {
        fs.promises.writeFile.mockResolvedValueOnce();
        const result = await writeChunkToFile(outputDir, chunkNumber, data);

        expect(fs.promises.writeFile).toHaveBeenCalledWith(
            outputFilePath,
            JSON.stringify(data, null, 2)
        );
        expect(console.info).toHaveBeenCalledWith(
            `✅ Created file: ${outputFilePath} with ${data.length} records.`
        );
        expect(result).toEqual({ fileName, writtenRecordsCount: data.length });
    });

    test('should throw an error when file writing fails', async () => {
        const errorMessage = 'Failed to write file';
        fs.promises.writeFile.mockRejectedValueOnce(new Error(errorMessage));
        await expect(writeChunkToFile(outputDir, chunkNumber, data)).rejects.toThrow(errorMessage);

        expect(fs.promises.writeFile).toHaveBeenCalledWith(
            outputFilePath,
            JSON.stringify(data, null, 2)
        );
        expect(console.error).toHaveBeenCalledWith(
            `❌ Error writing to file ${fileName}:`,
            errorMessage
        );
    });
});

// Test error handling for writeChunkToFile function
describe('writeChunkToFile error handling', () => {
    afterEach(() => {
        jest.clearAllMocks();
        jest.restoreAllMocks();
    });

    test('should throw and log error if fs.promises.writeFile fails', async () => {
        fs.promises.writeFile.mockRejectedValueOnce(new Error('fail'));
        const outputDir = '/tmp';
        const chunkNumber = 1;
        const data = [{ id: 1 }];
        jest.spyOn(Date.prototype, 'getFullYear').mockReturnValue(2025);
        jest.spyOn(Date.prototype, 'getMonth').mockReturnValue(4);

        await expect(writeChunkToFile(outputDir, chunkNumber, data)).rejects.toThrow('fail');
        expect(console.error).toHaveBeenCalled();
        jest.restoreAllMocks();
    });
});

// Test for generateKey function
describe('generateKey', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    })

    afterEach(() => {
        jest.restoreAllMocks();
    });

    test('should generate a valid chunk file name with the correct pattern', () => {
        const mockDate = new Date(2025, 4, 18);
        jest.spyOn(global, 'Date').mockImplementation(() => mockDate);
        const chunkNumber = 3;
        const expectedFileName = 'projectA-data-monthly-2025-05-chunk_3.json';
        const fileName = generateKey(chunkNumber);

        expect(fileName).toBe(expectedFileName);

    });

    test('should generate unique file names for different chunk numbers', () => {
        const fileName1 = generateKey(1);
        const fileName2 = generateKey(2);

        expect(fileName1).not.toBe(fileName2);
        expect(fileName1).toMatch(/^projectA-data-monthly-\d{4}-\d{2}-chunk_1\.json$/);
        expect(fileName2).toMatch(/^projectA-data-monthly-\d{4}-\d{2}-chunk_2\.json$/);
    });

    test('should correctly pad the month with a leading zero if necessary', () => {
        const mockDate = new Date(2025, 0, 10);
        jest.spyOn(global, 'Date').mockImplementation(() => mockDate);
        const fileName = generateKey(1);

        expect(fileName).toBe('projectA-data-monthly-2025-01-chunk_1.json');
    });
});


// Test for primaryTaskSplitLargeJsonIntoChunks function
describe('primaryTaskSplitLargeJsonIntoChunks', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.restoreAllMocks();
        // Mock fs.createReadStream for pipeline
        const { Readable } = require('stream');
        fs.createReadStream = jest.fn(() => {
            const readable = new Readable({
                objectMode: true,
                read() {
                    this.push('[{"id":1},{"id":2},{"id":3}]');
                    this.push(null);
                },
            });
            return readable;
        });

        // Mock console methods to prevent actual logs
        jest.spyOn(console, 'info').mockImplementation(() => { });
        jest.spyOn(console, 'error').mockImplementation(() => { });
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('creates output directory if it does not exist', async () => {
        fs.existsSync = jest.fn().mockReturnValue(false);
        fs.mkdirSync = jest.fn();
        await primaryTaskSplitLargeJsonIntoChunks();

        expect(fs.existsSync).toHaveBeenCalledWith(OUTPUT_DIR);
        expect(fs.mkdirSync).toHaveBeenCalledWith(OUTPUT_DIR);
    });

    it('should return keys to chunks if data integrity check passes', async () => {
        const fileName = generateKey(1);
        const result = await primaryTaskSplitLargeJsonIntoChunks();

        expect(fs.promises.writeFile).toHaveBeenCalledTimes(1);
        expect(result).toEqual({ keysToChunks: [fileName] });
    });
});