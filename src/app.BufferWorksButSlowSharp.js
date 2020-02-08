const _ = require('lodash');
const BluebirdPromise = require("bluebird");
const fs = require('fs');
const sharp = require('sharp');
const streamifier = require('streamifier');
const streamLength = require("stream-length");
const lengthStream = require('length-stream');
const stream = require('stream');
const isStream = require('is-stream');
const fsExtra = require('fs-extra');


const { getS3TileKeyPath, s3 } = require('./utils');

/* Returns all the tile transformers for a given zoom level. */
const zoomLevelTransformers = (sharpBigImageFileStream, zoomLevel, tileSize, zoomLevelsTiles) => {
    /* rootSize = 4096px x 4096px if zoomLevel is 3
       rootSize = 8192px x 8192px if zoomLevel is 4 
       So be carefull with the RAm and cpu 
    */
    const rootSize = tileSize * 2 ** zoomLevel;

    /* Resize the BigSquare image to root size of zoom level */
    const root = sharpBigImageFileStream.resize({
        width: rootSize,
        height: rootSize,
    });

    const tileColumnCount = 2 ** zoomLevel;

    return Promise.all(
        _().range(tileColumnCount)
            .flatMap(x => _.range(tileColumnCount)
                .map(y => {
                    console.log('Start — :', `Z-${zoomLevel}-${x}-${y}`);
                    const rootClone = root.clone();

                    let tileBuffer;

                    const cropedImage = rootClone.extract({
                        left: tileSize * x,
                        top: tileSize * y,
                        width: tileSize,
                        height: tileSize,
                    });

                    console.log('>>  1111');

                    return cropedImage
                        .toBuffer()
                        .then((data) => {
                            console.log('createdBuffer :', `Z-${zoomLevel}-${x}-${y}`);
                            tileBuffer = data;
                            const newTile = {
                                x,
                                y,
                                tileBuffer,
                                zoomLevel,
                            }
                            zoomLevelsTiles.push(newTile);
                        })
                        .catch(err => { 
                            console.log('rootClone.extract error :', err);
                        });
                })
            )
    )
};


exports.lambdaHandler = async (event) => {
    console.log('CLEAN FOLDER :');
    fsExtra.emptyDirSync('./dist');
    console.log('CLEAN FINISHED :');


    const { body } = event;
    console.log('Lambda body :', body);
    const { 
        blueprintsBucket, // Bucket of the blueprints
        fileBucketKeyPrefix,  // Keyprefix of the folder of the file in blueprint bucket
        pageNumber, // Page number of the pdf
        tileSize, // Size of a tile
        zoomLevel, // Total zoom levels allowed (saved in the file)
    } = body;

    /* Fetch the big square image from S3 */
    let bigCompressedSquareImageWithWhiteMargins;

    const bigImagePath = `${fileBucketKeyPrefix}/pages/${pageNumber}/big.png`;
    console.log('>>> bigImagePath :', bigImagePath);
    
    await s3.getObject({
            Bucket: blueprintsBucket,
            Key: `${fileBucketKeyPrefix}/pages/${pageNumber}/big.png`,
        })
        .promise()
        .then((data) => {
            console.log('>>> Upload success from S3 :', data);
            bigCompressedSquareImageWithWhiteMargins = new Buffer(data.Body);
        })
        .catch((err) => {
            console.log('>>> Upload error from S3 :', err);
        })

    console.log('>>> BBBBB :');
    const bigImageFileStream = await streamifier.createReadStream(bigCompressedSquareImageWithWhiteMargins);
    const sharpBigImageFileStream = sharp();
    sharpBigImageFileStream.setMaxListeners(0);
    /* stream 'bigImageFileStream' to 'processedBigImage' */
    bigImageFileStream.pipe(sharpBigImageFileStream);

    const zoomLevelRange =_.range(zoomLevel);

    console.log('>>>> GO TILES :');
    let zoomLevelsTiles = [];
    await BluebirdPromise.all(
        zoomLevelRange.map((zoomLevel) => {
            console.log('zoomLevel :', zoomLevel);
            return zoomLevelTransformers(sharpBigImageFileStream, zoomLevel, tileSize, zoomLevelsTiles);
        }
        ),
    )
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('zoomLevelsTiles :', zoomLevelsTiles);

    console.log('>>> CCCCC :');

    let promisesResult;
    await BluebirdPromise.all(
            zoomLevelsTiles.map(tile) => {
                const { x, y, zoomLevel, tileBuffer } = tile;

                const s3TileKeyPathToUpload = getS3TileKeyPath({ 
                    prefix: fileBucketKeyPrefix, 
                    pageNumber, 
                    x, 
                    y, 
                    zoomLevel,
                });

                const tileToUpload = {
                    Bucket: blueprintsBucket,
                    ACL: 'public-read',
                    Key: s3TileKeyPathToUpload,
                    Body: tileBuffer,
                    ContentType: 'image/png',
                    CacheControl: 'max-age=31556926',
                };

                console.log('tileToUpload :', tileToUpload.toString()); 
                
                const uploadPromise = s3.upload(
                    tileToUpload,
                    {
                        partSize: 5 * 1024 * 1024,
                        queueSize: 10,
                    },
                ).promise()

                return uploadPromise.then(function(data) {
                    console.log('>>> Success upload data tile:', `Z-${zoomLevel}-${x}-${y}`);
                }).catch(function(err) {
                    console.log('>>> Download error from S3 :', `Z-${zoomLevel}-${x}-${y}`);
                    console.log('>>> Download error from S3 :', err);
                });
            }),
    ).then((result) => {
        promisesResult = "Success"
    }).catch(err => {
        console.error(err);
        promisesResult = "Error"
    })


    return {
        body: promisesResult,
        statusCode: 200,
    };
};
