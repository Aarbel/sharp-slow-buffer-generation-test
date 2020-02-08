'use strict';

const app = require('../../app.js');
const chai = require('chai');
const expect = chai.expect;
const event = {
    body: {
        "fileBucketKeyPrefix": "362cb804-9a43-417a-9036-0b67303fbab7",
        "blueprintsBucket": "development-clovis-blueprints",
        "pageNumber": 1,
        "tileSize": 512,
        "zoomLevel": 5,
    },
};
let context;


describe('Tests index', function () {
    it('verifies successful response', async () => {
        const result = await app.lambdaHandler(event, context)
        console.log('result :', result);
        
        expect(result).to.be.an('object');
        expect(result.statusCode).to.equal(200);
        expect(result.body).to.be.an('string');;
        
        let response = result.body;
        
        expect(response).to.be.equal("Success");
    });
});
