import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient, BatchWriteItemCommand, ProvisionedThroughputExceededException } from '@aws-sdk/client-dynamodb';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

// This application reads json data files from an S3 bucket
// and puts them into a DynamoDB table as rows.

// This exceeds my aws throughput limits pretty quickly,
// so there is logic to retry and slow down the table uploads
// if we receive rate limit errors.
// Create the tables with On Demand billing to avoid.

const s3Bucket = process.env.EGV_RESOURCE_JSON_BUCKET;

const s3Client = new S3Client({
    region: process.env.AWS_REGION,
    credentials: fromNodeProviderChain(),
});

const dynamoTableName = process.env.EGV_RESOURCE_SEARCH_TABLE;

const dynamoClient = new DynamoDBClient({
    region: process.env.AWS_REGION,
    credentials: fromNodeProviderChain(),
});

async function main() {
    console.log("clearing table");
    await clearDynamoTable();

    console.log("enumerating s3 bucket");
    await enumerateBucket();
}

function elapsedSeconds(startTime) {
    const elapsed = process.hrtime.bigint() - startTime;
    return (parseFloat(elapsed) / 1e9).toFixed(2);
}

async function clearDynamoTable() {
    // TODO: doesn't clear the table first.
    // So currently orphan posts are possible.
    console.log('(clearing table not implemented yet)');
}

async function enumerateBucket() {
    const pages = [];
    const sections = [];
    let pageItemBatch = [];
    let sectionItemBatch = [];
    let continuationToken;
    do {
        const listParams = new ListObjectsV2Command({
            Bucket: s3Bucket,
            MaxKeys: 50,
            ContinuationToken: continuationToken,
        });
        const response = await s3Client.send(listParams);
        if (response.Contents) {
            for (const object of response.Contents) {
                const key = object.Key;
                if (!key.endsWith('/index.json')) continue;
                const data = JSON.parse(await getS3Object(key));
                const parts = key.split('/');
                const path = parts.slice(0, -1).join('/');
                if (data.children && data.children.length > 0) {
                    // List page, with children
                    const section = parts.slice(0, -1).join('/');
                    sections.push(canonicalize(section));
                    sectionItemBatch = writeSectionToBatch({
                        ...data,
                        key: key,
                        section: section,
                    }, sectionItemBatch);
                } else {
                    // Single page, no children
                    pages.push(canonicalize(path));
                    const section = parts.slice(0, -2).join('/');
                    pageItemBatch = writePageToBatch({
                        ...data,
                        key: key,
                        section: section,
                    }, pageItemBatch);
                }
            }
        }
        console.log(`found ${pages.length} pages and ${sections.length} sections`);
        continuationToken = response.NextContinuationToken;
    } while (continuationToken);
    await flushPageBatch(pageItemBatch);
    await flushSectionBatch(sectionItemBatch);
    console.log("finished enumerating bucket");
}

async function getS3Object(key) {
    const command = new GetObjectCommand({
        Bucket: s3Bucket,
        Key: key,
    });
    const response = await s3Client.send(command);
    if (response.Body) return response.Body?.transformToString();
    return undefined;
}

function writePageToBatch(data, batch) {
    batch.push(data);
    if (batch.length >= 20) {
        flushPageBatch(batch); // async
        batch = [];
    }
    return batch;
}

function searchContent(json) {
    // Concatenation of many things we could search for,
    // all converted to lower case.
    return [
        json.metadata && json.metadata.categories ? json.metadata.categories.join(' ').toLowerCase() : '',
        json.metadata && json.metadata.tags ? json.metadata.tags.join(' ').toLowerCase() : '',
        json.metadata && json.metadata.title ? json.metadata.title.toLowerCase() : '',
        json.metadata && json.metadata.summary ? json.metadata.summary.toLowerCase() : '',
        json.plain ? json.plain.toLowerCase() : '',
    ].join(' ');
}

async function flushPageBatch(batch) {
    const itemsToWrite = batch.map((json) => ({
        pagePath: { S: canonicalize(json.link) },
        pageSection: { S: canonicalize(json.section) },
        pageDate: { S: json.date },
        pageSearchContent: { S: searchContent(json) },
        objectKey: { S: json.key },
    }));
    return writeBatch(dynamoTableName, itemsToWrite);
}

function writeSectionToBatch(data, batch) {
    batch.push(data);
    if (batch.length >= 20) {
        flushSectionBatch(batch);
        batch = [];
    }
    return batch;
}

function flushSectionBatch(batch) {
    console.log("flushing section batch");
}

// Canonicalize a path (which is the table key)
function canonicalize(path) {
    try {
        let newPath = path;
        if (path === '') return '/';
        if (newPath && newPath.endsWith('/index.json')) {
            newPath = newPath.substring(0, newPath.length - '/index.json'.length);
        }
        if (newPath && !newPath.startsWith('/'))
            newPath = '/' + newPath;
        if (newPath && newPath.endsWith('/'))
            newPath = newPath.substring(0, newPath.length - 1);
        return newPath;
    } catch (error) {
        console.log('path:', path);
        throw error;
    }
}

function firstString(elements) {
    if (!elements) return "";
    if (elements.length < 1) return "";
    return elements[0];
}

function normalizeArray(elements) {
    if (!elements) return [];
    return elements;
}

async function writeBatch(tableName, itemsToWrite) {
    let backoff = 1500;
    if (!itemsToWrite || itemsToWrite.length === 0) return;
    console.log(`starting write of ${itemsToWrite.length} items to ${tableName}`);
    // write the batch of row items
    const batchWriteCommand = new BatchWriteItemCommand({
        RequestItems: {
            [tableName]: itemsToWrite.map((item) => ({
            PutRequest: {
                Item: item,
            },
            })),
        },
    });
    for (let retry = 0; retry < 20; retry++) {
        try {
            const response = await dynamoClient.send(batchWriteCommand);
            console.log(`completed writing to ${tableName}`);
            return;
        } catch (error) {
            if (error instanceof ProvisionedThroughputExceededException) {
                console.log(`throughput exceeded, retry ${retry+1}`);
                await new Promise((resolve) => setTimeout(resolve, backoff));
                backoff *= 2; // Exponential backoff          
            } else {
                console.log("error writing items:", error);
                throw error;
            }
        }
    }
}

// Lambda function handler when run by Lambda
export async function handler(event, context) {
    console.log(`starting endgameviable dbsync from ${s3Bucket} to ${dynamoTableName}`);
    const startTime = process.hrtime.bigint();
    await main();
    console.log(`finished in ${elapsedSeconds(startTime)} sec`);
}

// Invoke the handler if run directly on command line
if (require.main === module) {
    (async () => await this.handler(undefined, undefined))();
}
