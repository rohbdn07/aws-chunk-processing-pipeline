const { Readable } = require('stream');

const mockS3Send = jest.fn();

jest.mock('@aws-sdk/client-s3', () => {
    class GetObjectCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'GetObjectCommand';
        }
    }

    return {
        S3Client: jest.fn(() => ({ send: mockS3Send })),
        GetObjectCommand,
    };
});

const recordsToStream = (records) => Readable.from([JSON.stringify(records)]);

describe('sender Lambda handler', () => {
    let handler;

    beforeEach(() => {
        jest.resetModules();
        jest.clearAllMocks();
        jest.spyOn(console, 'info').mockImplementation(() => { });
        jest.spyOn(console, 'error').mockImplementation(() => { });
        process.env.BUCKET_NAME = 'report-store';

        ({ handler } = require('./index'));
    });

    afterEach(() => {
        jest.restoreAllMocks();
        delete process.env.BUCKET_NAME;
    });

    test('validates a chunk with player records', async () => {
        mockS3Send.mockResolvedValue({
            Body: recordsToStream([
                { player_id: 'player-1' },
                { player_id: 'player-2' },
            ]),
        });

        const result = await handler('projectA/data/monthly/2026/04/report/chunk_1.json');

        expect(mockS3Send).toHaveBeenCalledTimes(1);
        expect(mockS3Send.mock.calls[0][0].input).toMatchObject({
            Bucket: 'report-store',
            Key: 'projectA/data/monthly/2026/04/report/chunk_1.json',
        });
        expect(result).toEqual({
            validatedReportChunkSizes: [
                {
                    key: 'projectA/data/monthly/2026/04/report/chunk_1.json',
                    size: 2,
                },
            ],
        });
    });

    test('rejects invalid input that is not a chunk key string', async () => {
        await expect(handler({ keysToChunks: ['chunk_1.json'] })).rejects.toThrow(
            'Sender expects a single S3 chunk key string'
        );
        expect(mockS3Send).not.toHaveBeenCalled();
    });

    test('rejects a chunk with more than 3000 records', async () => {
        mockS3Send.mockResolvedValue({
            Body: recordsToStream(
                Array.from({ length: 3001 }, (_, index) => ({
                    player_id: `player-${index + 1}`,
                }))
            ),
        });

        await expect(handler('chunk_1.json')).rejects.toThrow('Expected up to 3000 records');
    });

    test('rejects a chunk record without player_id', async () => {
        mockS3Send.mockResolvedValue({
            Body: recordsToStream([
                { player_id: 'player-1' },
                { name: 'Missing player id' },
            ]),
        });

        await expect(handler('chunk_1.json')).rejects.toThrow('without player_id');
    });
});
