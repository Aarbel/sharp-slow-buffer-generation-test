'use strict';

const app = require('../../app.js');
const chai = require('chai');
const expect = chai.expect;


describe('Tests index', function () {
    it('verifies successful treatment', async () => {
        const result = await app.lambdaHandler()
        console.log('result :', result);
        
        expect(result).to.be.an('object');
        expect(result.statusCode).to.equal(200);
        expect(result.body).to.be.an('string');;
        
        let response = result.body;
        
        expect(response).to.be.equal("Success");
    });
});
