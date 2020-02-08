const _ = require('lodash');
const BluebirdPromise = require("bluebird");
const fs = require('fs');
const sharp = require('sharp');
const streamifier = require('streamifier');
const streamLength = require("stream-length");
const lengthStream = require('length-stream');
const stream = require('stream');
const isStream = require('is-stream');


const { getS3TileKeyPath, promisifedS3Methods, s3 } = require('./utils');

/* The source should be already resized.
   size: The width and the height of the output image. */
const tileTransformer = ({ pipeline, x, y, tileSize }) => pipeline.extract({
    left: tileSize * x,
    top: tileSize * y,
    width: tileSize,
    height: tileSize,
});


/* Returns all the tile transformers for a given zoom level. */
const zoomLevelTransformers = (bigSquareImage, zoomLevel, tileSize) => {
    /* rootSize = 4096px x 4096px if zoomLevel is 3
       rootSize = 8192px x 8192px if zoomLevel is 4 
       So be carefull with the RAm and cpu 
    */
    const rootSize = tileSize * 2 ** zoomLevel;

    /* Resize the BigSquare image to root size of zoom level */
    const root = bigSquareImage.resize({
        width: rootSize,
        height: rootSize,
    });

    const tileColumnCount = 2 ** zoomLevel;

    const allTiles = _().range(tileColumnCount)
        .flatMap(x => _.range(tileColumnCount)
            .map(y => {
                const newTile = {
                    x,
                    y,
                    stream: tileTransformer({
                        pipeline: root.clone(),
                        x,
                        y,
                        tileSize,
                    }),
                }

                console.log('newTile x:', x);
                console.log('newTile y:', y);

                return newTile;
            })
        )
        .value();

    return allTiles
};


exports.lambdaHandler = async (event) => {
    const { body } = event;
    console.log('Lambda body :', body);
    const { 
        blueprintsBucket, // Bucket of the blueprints
        fileBucketKeyPrefix,  // Keyprefix of the folder of the file in blueprint bucket
        pageNumber, // Page number of the pdf
        tileSize, // Size of a tile
        zoomLevelCount, // Total zoom levels allowed (saved in the file)
    } = body;

    console.log('>>> blueprintsBucket :', blueprintsBucket);
    console.log('>>> fileBucketKeyPrefix :', fileBucketKeyPrefix);

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

    const zoomLevelRange =_.range(zoomLevelCount);
    console.log('>>> CCCCC :');

    await BluebirdPromise.mapSeries(zoomLevelRange, function(zoomLevel) {
        /* Generate all the tiles and upload them to s3 for this zoom Level */
        console.log('zoomLevel begin :', zoomLevel);

        const zoomLevelsTiles = zoomLevelTransformers(sharpBigImageFileStream, zoomLevel, tileSize);

        /* Upload all the tiles to S3 after they are generated */
        return BluebirdPromise.mapSeries(
            zoomLevelsTiles, 
            ({ x, y, stream }) => {
                const s3TileKeyPathToUpload = getS3TileKeyPath({ prefix: fileBucketKeyPrefix, pageNumber, x, y, zoomLevel});
                console.log('>>> s3TileKeyPathToUpload :', s3TileKeyPathToUpload);

                const tileToUpload = {
                    Bucket: blueprintsBucket,
                    ACL: 'public-read',
                    Key: getS3TileKeyPath({ prefix: fileBucketKeyPrefix, pageNumber, x, y, zoomLevel}),
                    Body: stream,
                    ContentType: 'image/png',
                    CacheControl: 'max-age=31556926',
                };

                console.log('tileToUpload :', tileToUpload);

                
                const uploadPromise = s3.upload(
                    tileToUpload,
                    {
                        partSize: 5 * 1024 * 1024,
                        queueSize: 1,
                    },
                ).promise()

                return uploadPromise.then(function(data) {
                    console.log('>>> Success upload data:', data);
                }).catch(function(err) {
                    console.log('>>> Download error from S3 :', err);
                });
            }
        )
    }).then((result) => {
        console.log('Promise ALL  :');
    })


    return "HELLO"
};
