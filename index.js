const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { v4: uuidv4 } = require('uuid');
const sourcePath = './customers';
const conversationPath = './conversations';
const folderPath = './csv';
const targetLine = 5;

let customers = [];
let conversations = [];
let messages = [];
let conversationMessages = []

const readCSVFolder = async (folder) => {
  const files = await fs.promises.readdir(folder);
  const csvFiles = files.filter(file => path.extname(file).toLowerCase() === '.csv');

  return Promise.all(csvFiles.map(file => {
    const filePath = path.join(folder, file);
    return new Promise((resolve, reject) => {
      const results = [];

      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', row => results.push(row))
        .on('end', () => {
          resolve(results);
        })
        .on('error', reject);
    });
  })).then(fileDataArrays => fileDataArrays.flat());
};

const readCSVFolderWithLineFilter = async (folder, minLine = 1) => {
  const files = await fs.promises.readdir(folder);
  const csvFiles = files.filter(file => path.extname(file).toLowerCase() === '.csv');

  return Promise.all(csvFiles.map(file => {
    const filePath = path.join(folder, file);
    return new Promise((resolve, reject) => {
      const results = [];
      let rowIndex = 0;

      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', row => {
          rowIndex++;
          if (rowIndex >= minLine) {
            results.push(row);
          }
        })
        .on('end', () => {
          resolve(results);
        })
        .on('error', reject);
    });
  })).then(fileDataArrays => fileDataArrays.flat());
};

const main = async () => {
  try {
    console.log('📥 Loading customers...');
    customers = await readCSVFolder(sourcePath);
    console.log(`✅ Customers loaded: ${customers.length}`);

    console.log('📥 Loading conversations...');
    conversations = await readCSVFolder(conversationPath);
    console.log(`✅ Conversations loaded: ${conversations.length}`);

    console.log('📥 Processing message CSVs...');
    const outputRows = await readCSVFolderWithLineFilter(folderPath, targetLine);

    messages = outputRows.map(row => {
      const name = row.ALNEX || row.sender || row.Name || '';
      const message = row._4 || row.Message || row.content || '';
      const createdAt = `${row._2?.replaceAll('/', '-') || ''}T${row._3 || ''}`;

      const customer = customers.find(cs => cs.firstName === name);

    //   console.log({conversations })
      const conversation = conversations.find(con => con.customerId === customer?._id)

      const is3rdParty =  !customer?._id
      return { _id: `import_${uuidv4()}`, content: message, createdAt, customerId: customer?._id || null, userId: null, conversationId: conversation?._id || null, from3rdParty: is3rdParty };
    });

    conversationMessages = outputRows.map(row => {
      const message = row._4 || row.Message || row.content || '';
      const createdAt = `${row._2?.replaceAll('/', '-') || ''}T${row._3 || ''}`;

      const customer = customers.find(cs => cs.firstName === name);

    //   console.log({conversations })
      const conversation = conversations.find(con => con.customerId === customer?._id)

      const is3rdParty =  !customer?._id
      return { _id: `import_${uuidv4()}`, content: message, createdAt, customerId: customer?._id || null, userId: null, conversationId: conversation?._id || null, from3rdParty: is3rdParty, recipientId:  };

    })

    console.log(`📦 Total messages parsed: ${messages.length}`);
    fs.writeFileSync('messages.json', JSON.stringify(messages, null, 2));
    console.log('💾 File saved: output.json');
  } catch (err) {
    console.error('❌ Error:', err);
  }
};

main();
