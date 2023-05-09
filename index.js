// get list of chunks of a file
function getloc(arr, js){
    var blcs = [];
    if (arr.length == 1) {
        let key = arr[0];
        blcs = js[key];
    }
    else {
        blcs = this.getloc(arr.slice(1), js[arr[0]])
    }
    return blcs;
}

function loadFile(fpath) {
    // get file content
    fetch(fpath)
      .then(response => {
        // create a new ReadableStream from the response body
        const stream = response.body.getReader();
  
        // function to process each chunk of the file
        function processChunk({ done, value }) {
          if (done) {
            // when all chunks have been read, return the concatenated content
            return accumulatedContent;
          }
  
          // decode and concatenate
          const textDecoder = new TextDecoder('utf-8');
          const decodedChunk = textDecoder.decode(value);
          accumulatedContent += decodedChunk;
  
          // read the next chunk
          return stream.read().then(processChunk);
        }
  
        // string to store contents
        let accumulatedContent = '';
  
        // read contents
        return stream.read().then(processChunk).then(content => {
          // create a pop-up window to display file content
          const newWindow = window.open('', '_blank');
          newWindow.document.write('<pre>' + content + '</pre>');
        });
      })
      .catch(error => {
        //error handling
        console.error(error);
      });
  }

function displayJsonTree(){
    var jsData = null;
    fetch('./NameNode/records.json')
    .then(response => response.json())
    .then(json => {
        // create TreeView
        const treeView = new TreeView('#treeView', json);
        jsData = json;
        treeView.render();
    })
    .catch(error => console.error(error));

    class TreeView {
        constructor(selector, data) {
            this.$el = document.querySelector(selector);
            this.data = data;
        }

        render() {
            this.$el.innerHTML = '/'+this.buildTree(this.data,'','');
            this.$el.querySelectorAll('.file').forEach((toggle) => {
            toggle.addEventListener('click', (event) => {
                // if the user clicked on a block, call loadFile to display the content in new window
                var p = toggle.getAttribute('data-path');
                var segs = p.split('/');
                var num = parseInt(toggle.innerText.split(' ').slice(-1)[0]);
                var info = getloc(segs.slice(1), jsData);
                var name = info[num-1][0];
                name = name + '.' + p.split('.')[1];
                var folder = info[num-1][1];
                folder = './DataNode'+folder;
                loadFile(folder+'/'+name);

            });
            });
            this.$el.querySelectorAll('.filename').forEach((toggle) => {
            toggle.addEventListener('click', (event) => {
                // if user clicked on a file, update the show path to the file in the corresponding box and update text on the button
                const ele = document.querySelector('.epath');
                var p = toggle.getAttribute('data-path');
                if (p!='/') {
                    ele.textContent = p+'/'+toggle.innerText;
                }
                else {
                    ele.textContent = p+toggle.innerText;
                }
                document.querySelector('.button').textContent = 'Download';

            });
            });
            this.$el.querySelectorAll('.dir').forEach((toggle) => {
            toggle.addEventListener('click', (event) => {
                // if user clicked on a directory, update the show path to the dir in the corresponding box and update text on the button
                const ele = document.querySelector('.epath');
                var p = toggle.getAttribute('data-path');
                if (p!='/') {
                    ele.textContent = p+'/'+toggle.innerText;
                }
                else {
                    ele.textContent = p+toggle.innerText;
                }
                document.querySelector('.button').textContent = 'Upload';
            });
            });
        }

        // build tree according to records.json
        buildTree(data, fname, path) {
            let tree = '';
            if (Array.isArray(data[0])) {
                data = data.slice(0,-1);
            }
            for (let key in data) {
            if (typeof data[key] === 'object') {
                // add blocks, class='file'
                if (/^\d+$/.test(key)) {
                    tree += `<li><a href='#' class="file" data-path=${path}>Block ${parseInt(key)+1}${this.buildTree(data[key].slice(0, -1),'',path)}</a></li>`;
                }
                else if (Array.isArray(data[key])){
                    // add file names, class='filename'
                    if (!path) {
                        tree += `<li><a href='#' class="filename" data-path='/'>${key}<a><ul>${this.buildTree(data[key], key, path+'/'+key)}</ul></li>`;
                    }
                    else {
                        tree += `<li><a href='#' class="filename" data-path=${path}>${key}<a><ul>${this.buildTree(data[key], key, path+'/'+key)}</ul></li>`;
                    }
                }
                else { 
                    // add directories, class='dir'
                    if (!path) {
                        tree += `<li><a href='#' class="dir" data-path='/'>${key}</a><ul>${this.buildTree(data[key], key, path+'/'+key)}</ul></li>`;
                    }
                    else {
                        tree += `<li><a href='#' class="dir" data-path=${path}>${key}</a><ul>${this.buildTree(data[key], key, path+'/'+key)}</ul></li>`;
                    }
                }
            } 
            }
            return `<ul>${tree}</ul>`;
        }
    }
}
// handle button-clicking event
function buttonResponse(){
    const text = document.querySelector('.button').innerText;
    const localp = document.querySelector('#lpath').innerText;
    // check if the user have input path to file on local device; if not, do nothing
    if (localp !== 'Manually input path to your destination here. For files, also add the desired file name to the path.') {
        if (text !== 'fill in the paths first') {
            const edfsp = document.querySelector('#epath').innerText;
            // upload file
            if (text === 'Upload') {
                // run Python script using [python edfs.py -put localp edfsp]
                const url = new URL('http://localhost:3000/edfs');
                url.searchParams.append('firstParam', encodeURIComponent('-put'));
                url.searchParams.append('secondParam', encodeURIComponent(localp));
                url.searchParams.append('thirdParam', encodeURIComponent(edfsp));

                fetch(url)
                .then(response => response.text())
                .then(data => {
                    console.log(data);
                })
                .catch(error => console.error(error))
                displayJsonTree();}
            // download file
            else if (text === 'Download') {
                // run Python script using [python edfs.py -get edfsp localp]
                const url = new URL('http://localhost:3000/edfs');
                url.searchParams.append('firstParam', encodeURIComponent('-get'));
                url.searchParams.append('secondParam', encodeURIComponent(edfsp));
                url.searchParams.append('thirdParam', encodeURIComponent(localp));

                fetch(url)
                .then(response => response.text())
                .then(data => {
                    console.log(data);
                })
                .catch(error => console.error(error));};
}
}}
