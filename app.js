const _ = require('lodash');
const Promise = require("bluebird");
const fs = require('fs');
const sharp = require('sharp');
const fsExtra = require('fs-extra');


/* Returns all the tile transformers for a given zoom level. */
const zoomLevelTransformers = (sharpBigImageFileStream, zoomLevel) => {
    /* Using tiles of 512 x 512 */
    const rootSize = 512 * 2 ** zoomLevel;

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
                    console.log('Start — ', `Z-${zoomLevel}-${x}-${y}`);
                    const rootClone = root.clone();

                    const cropedImage = rootClone.extract({
                        left: 512 * x,
                        top: 512 * y,
                        width: 512,
                        height: 512,
                    });

                    return cropedImage
                        .toBuffer()
                        .then((data) => {
                            console.log('createdBuffer success — ', `Z-${zoomLevel}-${x}-${y}`);
                        })
                        .catch(err => { 
                            console.log('createdBuffer error :', err);
                        });
                })
            )
    )
};

exports.lambdaHandler = async () => {
    console.log('>>> Stream big.png image');
    const bigImageFileStream = await fs.createReadStream('big.png');
    const sharpBigImageFileStream = sharp();
    sharpBigImageFileStream.setMaxListeners(0);
    bigImageFileStream.pipe(sharpBigImageFileStream);

    const zoomLevelRange = [0, 1, 2, 3, 4];

    console.log('>>> Start cutting image into Buffer tiles');
    await Promise.all(
        zoomLevelRange.map((zoomLevel) => {
            console.log('zoomLevel :', zoomLevel);
            return zoomLevelTransformers(sharpBigImageFileStream, zoomLevel);
        }
        ),
    )
    console.log('>>> Wohoo, job finished !');

    return {
        body: "Success",
        statusCode: 200,
    };
};
