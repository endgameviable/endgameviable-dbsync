const { CreateTableCommand } = require('@aws-sdk/client-dynamodb');
const { dynamoClient } = require('./common');

const pageTableParams = {
    TableName: 'endgameviable-generated-pages',
    BillingMode: 'PAY_PER_REQUEST',
    // ProvisionedThroughput: {
    //     ReadCapacityUnits: 1,
    //     WriteCapacityUnits: 1,
    // },
    AttributeDefinitions: [
        { AttributeName: 'pagePath', AttributeType: 'S' },
        { AttributeName: 'pageDate', AttributeType: 'S' },
        { AttributeName: 'pageTitle', AttributeType: 'S' },
        { AttributeName: 'pageSummary', AttributeType: 'S' },
        { AttributeName: 'pageContentHtml', AttributeType: 'S' },
        { AttributeName: 'pageSection', AttributeType: 'S' },
        // { AttributeName: 'pageImages', AttributeType: 'L' },
        // { AttributeName: 'pageTags', AttributeType: 'L' },
        // { AttributeName: 'pageCategories', AttributeType: 'L' },
    ],
    KeySchema: [
        { AttributeName: 'pagePath', AttributeType: 'S', KeyType: 'HASH' },
    ],
};

// "slug" .Slug
// "link" .RelPermalink 
// "date" .Date
// "title" .Title
// "summary" (.Summary | plainify)
// "content" .Content
// "contentWordCount" .WordCount
// "contentReadingTime" .ReadingTime
// "tableOfContents" .TableOfContents
// "categories" $categories
// "alternates" $alternates
// "images" $images
// "tags" $tags
// "kind" .Kind
// "section" .Section
// "type" .Type


// It's a pain to deal with async functions in node.js
// when you don't want to call them asynchronously.
(async () => {
    console.log("starting");
    const startTime = performance.now();
    await main();
    const elapsed = (performance.now() - startTime) / 1000;
    console.log(`finished in ${elapsed.toFixed(2)}sec`);
})();

async function main() {
    console.log("creating page table");
    await createPageTable();
}

async function createPageTable() {
    const response = await dynamoClient.send(
        new CreateTableCommand(pageTableParams));
    console.log(response);
}
