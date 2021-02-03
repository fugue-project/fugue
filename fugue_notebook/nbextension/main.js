define([
	'require',
	'jquery',
	'base/js/namespace',
	'notebook/js/cell',
	'notebook/js/codecell',
	'codemirror/lib/codemirror'
], function (
	requirejs,
	$,
	Jupyter,
	cell,
	codecell,
	CodeMirror
) {
    "use strict";

    function set(str) {
        var obj = {}, words = str.split(" ");
        for (var i = 0; i < words.length; ++i) obj[words[i]] = true;
        return obj;
      }

    var fugue_keywords = "fill hash rand even presort persist broadcast params process output outtransform rowcount concurrency prepartition zip print title save append parquet csv json single checkpoint weak strong deterministic yield connect sample seed take sub callback dataframe file";

    function load_extension() {

        CodeMirror.defineMIME("text/x-fsql", {
            name: "sql",
            keywords: set(fugue_keywords + " add after all alter analyze and anti archive array as asc at between bucket buckets by cache cascade case cast change clear cluster clustered codegen collection column columns comment commit compact compactions compute concatenate cost create cross cube current current_date current_timestamp database databases data dbproperties defined delete delimited deny desc describe dfs directories distinct distribute drop else end escaped except exchange exists explain export extended external false fields fileformat first following for format formatted from full function functions global grant group grouping having if ignore import in index indexes inner inpath inputformat insert intersect interval into is items join keys last lateral lazy left like limit lines list load local location lock locks logical macro map minus msck natural no not null nulls of on optimize option options or order out outer outputformat over overwrite partition partitioned partitions percent preceding principals purge range recordreader recordwriter recover reduce refresh regexp rename repair replace reset restrict revoke right rlike role roles rollback rollup row rows schema schemas select semi separated serde serdeproperties set sets show skewed sort sorted start statistics stored stratify struct table tables tablesample tblproperties temp temporary terminated then to touch transaction transactions transform true truncate unarchive unbounded uncache union unlock unset use using values view when where window with"),
            builtin: set("date datetime tinyint smallint int bigint boolean float double string binary timestamp decimal array map struct uniontype delimited serde sequencefile textfile rcfile inputformat outputformat"),
            atoms: set("false true null"),
            operatorChars: /^[*\/+\-%<>!=~&|^]/,
            dateSQL: set("time"),
            support: set("ODBCdotTable doubleQuote zerolessFloat")
          });

        // Learned from: https://github.com/AmokHuginnsson/huginn/blob/86a5710f3a2495a0ebe38a95710d000349f9b965/src/codemirror.js
        CodeMirror.modeInfo.push( {
            name: "Fugue SQL",
            mime: "text/x-fsql",
            mode: "sql"
          } );

        Jupyter.notebook.config.loaded.then(function() {
            require(['notebook/js/codecell'], function(codecell) {
                codecell.CodeCell.options_default.highlight_modes['magic_text/x-fsql'] = {'reg':[/%%fsql/]} ;
                Jupyter.notebook.events.on('kernel_ready.Kernel', function(){
                Jupyter.notebook.get_cells().map(function(cell){
                    if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
                });
              });
        }).catch(function on_error (reason) { console.error('fugue_notebook', 'error loading:', reason); });
    };

    return {load_ipython_extension : load_extension};
});
