const csv = require('csv');
const fs = require('fs');

const parseCsvFile = (csvFile) => {
    return new Promise((res, rej) => {
        fs.readFile(csvFile, 'utf8', function (err, data) {
            try {
                var dataWithHeader = data.split(/\r?\n/);
                const headerRow = dataWithHeader[0];
                const headers = headerRow.split(',');
                const dataRows = dataWithHeader.slice(1);
                // console.log(JSON.stringify(headers));
                // console.log(JSON.stringify(dataRows))
                const jsonData = dataRows.map(r => {
                    const jsonItem = {}
                    const values = r.split(',');
                    for (let i = 0; i < values.length; i++) {
                        jsonItem[headers[i]] = values[i];
                    }
                    return jsonItem

                });
                res(jsonData);
            } catch (ex) {
                rej(ex);
            }
        });

    })

}
// const parseCSV = (csvFile) => {
//     const promise = new Promise((resolve, reject) => {

//         const csvData = fs.readFileSync(csvFile);
//         const records = [];
//         fs.createReadStream(csvFile)
//         .pipe(csv.parse({
//             delimiter: ','
//         })).pipe(x => {
//             records.push(x);
//         })
//         .on("end", () => {
//             console.log("finished");
//             resolve(JSON.stringify(records));
//           });
//     });

// }

module.exports = { parseCsvFile }