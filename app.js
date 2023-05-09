const express = require('express');
const app = express();
const cors = require('cors');
const { exec } = require('child_process');

// Enable CORS for all routes
app.use(cors());

// Serve static files from the public directory
app.use(express.static('public'));


app.get('/edfs', (req, res) => {
  // get parameters and format the command
  const firstParam = decodeURIComponent(req.query.firstParam);
  const secondParam = decodeURIComponent(req.query.secondParam);
  const thirdParam = decodeURIComponent(req.query.thirdParam);
  let command =`python edfs.py ${firstParam} ${secondParam} ${thirdParam}`;
  console.log(command);

  // error handling
  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      res.status(500).send('An error occurred while running the script');
    } else if (stderr) {
      console.error(`stderr: ${stderr}`);
      res.status(500).send('An error occurred while running the script');
    } else {
      console.log(`stdout: ${stdout}`);
      res.send(stdout);
    }
  });
});


// Start the server
const port = 3000;
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
