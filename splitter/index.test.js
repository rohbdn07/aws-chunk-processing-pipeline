const { Readable } = require('stream');

const mockS3Send = jest.fn();
const mockSnsSend = jest.fn();
const mockSqsSend = jest.fn();

jest.mock('@aws-sdk/client-s3', () => {
    class GetObjectCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'GetObjectCommand';
        }
    }

    class PutObjectCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'PutObjectCommand';
        }
    }

    class DeleteObjectsCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'DeleteObjectsCommand';
        }
    }

    return {
        S3Client: jest.fn(() => ({ send: mockS3Send })),
        GetObjectCommand,
        PutObjectCommand,
        DeleteObjectsCommand,
    };
});

jest.mock('@aws-sdk/client-sns', () => {
    class PublishCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'PublishCommand';
        }
    }

    return {
        SNSClient: jest.fn(() => ({ send: mockSnsSend })),
        PublishCommand,
    };
});

jest.mock('@aws-sdk/client-sqs', () => {
    class SendMessageCommand {
        constructor(input) {
            this.input = input;
            this.commandName = 'SendMessageCommand';
        }
    }

    return {
        SQSClient: jest.fn(() => ({ send: mockSqsSend })),
        SendMessageCommand,
    };
});

const recordsToStream = (records) => Readable.from([JSON.stringify(records)]);

describe('splitter Lambda handler', () => {
    let handler;

    beforeEach(() => {
        jest.resetModules();
        jest.clearAllMocks();
        jest.spyOn(console, 'info').mockImplementation(() => { });
        jest.spyOn(console, 'log').mockImplementation(() => { });
        jest.spyOn(console, 'error').mockImplementation(() => { });
        process.env.SQS_URL = 'http://localhost:4566/000000000000/my-sqs-queue';
        process.env.SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-1:000000000000:splitter_status';

        ({ handler } = require('./index'));
    });

    afterEach(() => {
        jest.restoreAllMocks();
        delete process.env.SQS_URL;
        delete process.env.SNS_TOPIC_ARN;
    });

    test('uploads only valid unique players and reports skipped counts', async () => {
        const inputRecords = [
            { player_id: 'player-1', name: 'Ada' },
            { name: 'Missing player id' },
            { player_id: 'player-1', name: 'Duplicate Ada' },
            { player_id: 'player-2', name: 'Grace' },
        ];

        mockS3Send.mockImplementation(async (command) => {
            if (command.commandName === 'GetObjectCommand') {
                return { Body: recordsToStream(inputRecords) };
            }
            if (command.commandName === 'PutObjectCommand') {
                return {};
            }
            throw new Error(`Unexpected S3 command: ${command.commandName}`);
        });
        mockSqsSend.mockResolvedValue({ MessageId: 'message-1' });
        mockSnsSend.mockResolvedValue({});

        const result = await handler({
            bucket: 'report-store',
            key: 'rud/monthly/2024/11/report.json',
        });

        const putCommands = mockS3Send.mock.calls
            .map(([command]) => command)
            .filter((command) => command.commandName === 'PutObjectCommand');

        expect(putCommands).toHaveLength(1);
        expect(JSON.parse(putCommands[0].input.Body)).toEqual([
            { player_id: 'player-1', name: 'Ada' },
            { player_id: 'player-2', name: 'Grace' },
        ]);
        expect(result.validRecordCount).toBe(2);
        expect(result.invalidRecordCount).toBe(1);
        expect(result.duplicateRecordCount).toBe(1);
        expect(result.keysToChunks).toHaveLength(1);
        expect(mockSqsSend).toHaveBeenCalledTimes(1);
        expect(mockSnsSend).toHaveBeenCalledTimes(1);
    });

    test('creates multiple chunks when valid unique player count exceeds 3000', async () => {
        const inputRecords = Array.from({ length: 3001 }, (_, index) => ({
            player_id: `player-${index + 1}`,
        }));

        mockS3Send.mockImplementation(async (command) => {
            if (command.commandName === 'GetObjectCommand') {
                return { Body: recordsToStream(inputRecords) };
            }
            if (command.commandName === 'PutObjectCommand') {
                return {};
            }
            throw new Error(`Unexpected S3 command: ${command.commandName}`);
        });
        mockSqsSend.mockResolvedValue({ MessageId: 'message-1' });
        mockSnsSend.mockResolvedValue({});

        const result = await handler({
            bucketName: 'report-store',
            key: 'rud/monthly/2024/11/report.json',
        });

        const putCommands = mockS3Send.mock.calls
            .map(([command]) => command)
            .filter((command) => command.commandName === 'PutObjectCommand');

        expect(putCommands).toHaveLength(2);
        expect(JSON.parse(putCommands[0].input.Body)).toHaveLength(3000);
        expect(JSON.parse(putCommands[1].input.Body)).toHaveLength(1);
        expect(result.validRecordCount).toBe(3001);
        expect(result.invalidRecordCount).toBe(0);
        expect(result.duplicateRecordCount).toBe(0);
        expect(result.keysToChunks).toHaveLength(2);
    });

    test('rolls back uploaded chunks when SQS notification for a chunk fails', async () => {
        const inputRecords = [{ player_id: 'player-1' }];

        mockS3Send.mockImplementation(async (command) => {
            if (command.commandName === 'GetObjectCommand') {
                return { Body: recordsToStream(inputRecords) };
            }
            if (command.commandName === 'PutObjectCommand') {
                return {};
            }
            if (command.commandName === 'DeleteObjectsCommand') {
                return { Deleted: command.input.Delete.Objects };
            }
            throw new Error(`Unexpected S3 command: ${command.commandName}`);
        });
        mockSqsSend.mockRejectedValue(new Error('SQS unavailable'));

        await expect(handler({
            bucket: 'report-store',
            key: 'rud/monthly/2024/11/report.json',
        })).rejects.toThrow('chunk upload task');

        const deleteCommands = mockS3Send.mock.calls
            .map(([command]) => command)
            .filter((command) => command.commandName === 'DeleteObjectsCommand');

        expect(deleteCommands).toHaveLength(1);
        expect(deleteCommands[0].input.Delete.Objects).toHaveLength(1);
    });
});
