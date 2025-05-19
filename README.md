# Assignment

Welcome to the assignment! This task is part of the hiring process for the Compliance Tech Team at Paf. We hope you find it engaging and rewarding.  

Please take a moment to carefully review the instructions below. If you have any questions about the requirements or encounter issues with the environment, feel free to reach out to us.  

Happy coding, and good luck!

---

## Introduction

Since Paf operates in regulated markets, we are obligated to submit regulatory reports to the authorities. The ComplianceTech team at Paf is responsible for generating and submitting all necessary regulatory reports to the respective authorities.

Our reporting pipelines run in AWS using cloud services like AWS Lambda, Amazon API Gateway, Amazon DynamoDB and etc. to implement serverless architectural patterns that reduce the operational complexity of running and managing applications.

We noticed one issue in one of our reporting pipelines. At the beginning of every month, we need to generate a monthly report in JSON format to be uploaded in a designated location of a S3 bucket. The generated report is an array of JSON objects, like the sample shown below.
```json
[
    {"id": "1", "date": "2025-01-10T12:45:23.254Z"},
    {"id": "2", "date": "2025-01-02T10:41:03.976Z"}
]
```
The size of the uploaded JSON file can be very large (e.g. > 350 MB). 

One dedicated Lambda function, named LF1, is required to read the content of the monthly report from S3 and then process each of the JSON objects. Due to the maximum timeout limit of 15 minutes in Lambda functions, LF1 might not be able to complete processing all the JSON objects in the monthly report.

A solution to workaround the timeout limitation is to split the monthly report into multiple smaller size JSON files. Lambda function, LF1, can then iteratively process each one of the smaller size JSON files within 15 minutes.

This is the background information of the assignment exercise.

---

## Primary task

The primary task involves splitting a large JSON file into multiple smaller JSON files. The following requirements must be met:

1. **Input:**
    - A single JSON file located at the following path: `./data/mock-rud.json`. This file contains a list of player records.
2. **Output:**
    - Multiple JSON files, each containing a subset of the player records.
3. **Constraints:**
    - Each output JSON file must contain a maximum of 3000 player records.
    - The splitting process should ensure that no player records are lost or duplicated.
4. **Goal:**
    - Efficiently handle the splitting process to produce properly structured JSON files while adhering to the maximum record limit.
5. **Unit testing:**
    - Choose a testing framework and implement some unit tests on your splitter implementation.

Ensure that the solution is robust, scalable, and capable of handling varying file sizes and record counts.

## Optional Task

This optional task is designed to assess your familiarity with, or willingness to learn, AWS services. Your objective is to implement the solution for the main task as an **AWS Lambda function**.  

The current StepFunction workflow in this assignment has only 2 steps as shown in this [diagram](./pics/StepFunctionWorkflow.png). And they are executed sequentially, i.e. SplitLargeReport and then SendReportByChunk.
* Step `SplitLargeReport` - the splitter Lambda function
* Step `SendReportByChunks`- the sender Lambda function

The output, `keysToChunks` array, from the execution of SplitLargeReport step is made available as input to the SendReportByChunks step. The sender Lambda function in the SendReportByChunk step is triggered by the StepFunction and processes the entire keyToChunks array in one execution.

Can you propose the necessary changes in the StepFunction workflow and the sender Lambda function, if needed, in order to process each item in the keysToChunks array by the sender Lambda function iteratively?

For example, the keysToChunks array has 3 elements:
```json
["key1", "key2", "key3"]
```
The sender Lambda function will be triggered by StepFunction 3 times sequentially. Each execution of the sender Lambda function will be provided with a key value from keysToChunks array (i.e. 'key1' / 'key2' /'key3') as the input to the sender Lambda function.

Your proposed solution can be documented in the given workflow file, `optional-task.asl.json`, using Amazon States Language (also known as ASL). You don't need to implement the changes in the sender Lambda function for this optional task. You can just provide a short description on the necessary changes in the sender Lambda function.


### Task Details
The Lambda function will serve as the first step in a state machine. It should:
1. Download the large report file from `report-store` S3 bucket.
1. Split the file into smaller chunks.  
1. Upload these chunks to the `report-store` S3 bucket.  
1. Return the S3 keys of the chunks in an array of strings under the `keysToChunks` property.  

Here’s an example of the expected output:  
```json
{
    "keysToChunks": ["key1", "key2", "key3"]
}
```

**Note:**
You are free to use the programming language of your choice to complete this assignment, as long as the solution meets the requirements specified.

### Pre-requisites

Before setting up the environment for this assignment, ensure that the following tools are installed on your system:  
- **Docker**
- **AWS CLI** 

Make sure these tools are properly configured and accessible from your terminal.

### Setup for the assignment

1. Navigate to the assignment's root directory.
2. Package the Lambda functions:
```bash
$ docker compose run --rm packager bash -c 'mkdir -p /src/build && rm -f /build/*.zip && cd /src/sender && zip -r /src/build/sender_function.zip * && cd ../splitter && zip -r /src/build/splitter_function.zip *'
```
3. Start LocalStack:
```bash
$ ./start-localstack.sh
```
4. Execute the state machine to verify the setup:
```bash
$ aws --endpoint-url=http://localhost:4566 stepfunctions start-execution --state-machine-arn "arn:aws:states:eu-west-1:000000000000:stateMachine:SplitAndSendReportByChunks" --input '{"bucket": "report-store", "key": "rud/monthly/2024/11/202411-123456789-RUD.json"}'
```
5. Check the Docker container logs to ensure the execution runs. NOTE: since the `splitter` function returns a dummy response, the execution fails with `The specified key does not exist.` error message. Your solution must fix it.

**Note:**  
If you choose a programming language different from the one used in the dummy `splitter` function provided in this setup, you will need to update the `start-localstack.sh` file. Specifically, modify the `runtime` parameter for the `splitter` Lambda function to match your chosen programming language.


### When your solution is ready do the following

1. Compile your solution and install any dependencies.
1. Package the solution into a ZIP file (refer to the setup command above for packaging).
1. Update the **splitter** Lambda function:
```bash
$ docker compose run --rm aws-cli --endpoint-url=http://localstack:4566 lambda update-function-code --function-name splitter --region eu-west-1 --zip-file fileb:///build/splitter_function.zip
```

## Optional design questions

1. Are the instructions for setting up the assignment clear to you? If not, how would you improve them?

> **Answer:**  
> The instructions are generally clear and well-structured, especially for someone familiar with Docker, AWS CLI, and serverless workflows. The step-by-step setup and packaging commands are helpful.  
> However, I encountered issues when running the `./start-localstack.sh` script: it was unable to create the Lambda functions, invoke them, and create the Step Function as expected. These steps had to be performed manually.  
> After the `put-object` command for the S3 bucket, the script stops or hangs. This is likely because a previous command (such as `aws s3api put-object`) is waiting for input or has failed, but the script does not have `set -e` to exit on error, nor does it print errors by default.  
> Additionally, if Docker or LocalStack is not fully ready, or if the AWS CLI cannot connect to LocalStack, the script may hang or fail silently at that point.

> **Suggestions for improvement:**
> - Add `set -e` at the top of the `start-localstack.sh` script to ensure the script exits immediately if any command fails, making errors more visible and preventing the script from hanging unexpectedly.
> - Explicitly mention where to find or set the AWS credentials for LocalStack, as some users may not have them configured.
> - Clarify the expected directory structure after running the packaging commands (e.g., where the ZIP files will appear).


2. The size of the JSON file in the assignment is not big at all. Do you think your splitting solution can handle a much larger file size (e.g. 500MB)?

> **Answer:**  
> Yes, my splitting solution is designed to handle very large files, including those of 500MB or more.  
> The implementation uses streaming and chunked processing, so it does not load the entire file into memory at once.  
> Instead, it reads and processes the JSON data in a memory-efficient way, splitting and writing each chunk as it goes.  
> This approach ensures scalability and stability even with very large input files, as memory usage remains low and predictable regardless of file size.

---

## Running the Tests

Unit tests are provided for the `primaryTasksplitLargeJson.js` to ensure correctness and reliability.

To run the tests:

1. Open a terminal and navigate to the `splitter` directory:
    ```bash
    cd splitter
    ```

2. Run the test suite using npm:
    ```bash
    npm run test
    ```

This will execute all test cases and display the results in your terminal.

