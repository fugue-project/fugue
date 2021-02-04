# flake8: noqa
from typing import Any

from fugue_version import __version__
from IPython import get_ipython
from IPython.display import Javascript

from fugue_notebook.env import NotebookSetup, _setup_fugue_notebook

_HIGHLIGHT_JS = r"""
require(["codemirror/lib/codemirror"]);
function set(str) {
    var obj = {}, words = str.split(" ");
    for (var i = 0; i < words.length; ++i) obj[words[i]] = true;
    return obj;
  }
var fugue_keywords = "fill hash rand even presort persist broadcast params process output outtransform rowcount concurrency prepartition zip print title save append parquet csv json single checkpoint weak strong deterministic yield connect sample seed take sub callback dataframe file";
CodeMirror.defineMIME("text/x-fsql", {
    name: "sql",
    keywords: set(fugue_keywords + " add after all alter analyze and anti archive array as asc at between bucket buckets by cache cascade case cast change clear cluster clustered codegen collection column columns comment commit compact compactions compute concatenate cost create cross cube current current_date current_timestamp database databases data dbproperties defined delete delimited deny desc describe dfs directories distinct distribute drop else end escaped except exchange exists explain export extended external false fields fileformat first following for format formatted from full function functions global grant group grouping having if ignore import in index indexes inner inpath inputformat insert intersect interval into is items join keys last lateral lazy left like limit lines list load local location lock locks logical macro map minus msck natural no not null nulls of on optimize option options or order out outer outputformat over overwrite partition partitioned partitions percent preceding principals purge range recordreader recordwriter recover reduce refresh regexp rename repair replace reset restrict revoke right rlike role roles rollback rollup row rows schema schemas select semi separated serde serdeproperties set sets show skewed sort sorted start statistics stored stratify struct table tables tablesample tblproperties temp temporary terminated then to touch transaction transactions transform true truncate unarchive unbounded uncache union unlock unset use using values view when where window with"),
    builtin: set("date datetime tinyint smallint int bigint boolean float double string binary timestamp decimal array map struct uniontype delimited serde sequencefile textfile rcfile inputformat outputformat"),
    atoms: set("false true null"),
    operatorChars: /^[*\/+\-%<>!=~&|^]/,
    dateSQL: set("time"),
    support: set("ODBCdotTable doubleQuote zerolessFloat")
  });

CodeMirror.modeInfo.push( {
            name: "Fugue SQL",
            mime: "text/x-fsql",
            mode: "sql"
          } );

require(['notebook/js/codecell'], function(codecell) {
    codecell.CodeCell.options_default.highlight_modes['magic_text/x-fsql'] = {'reg':[/%%fsql/]} ;
    Jupyter.notebook.events.on('kernel_ready.Kernel', function(){
    Jupyter.notebook.get_cells().map(function(cell){
        if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
  });
"""


def load_ipython_extension(ip: Any) -> None:
    """Entrypoint for IPython %load_ext"""
    _setup_fugue_notebook(ip, None)


def _jupyter_nbextension_paths():
    """Entrypoint for Jupyter extension"""
    return [
        {
            "section": "notebook",
            "src": "nbextension",
            "dest": "fugue_notebook",
            "require": "fugue_notebook/main",
        }
    ]


def setup(notebook_setup: Any = None) -> Any:
    """Setup the notebook environment inside notebook without
    installing the jupyter extension or loading ipython extension

    :param notebook_setup: ``None`` or an instance of
      :class:`~.fugue_notebook.env.NotebookSetup`, defaults to None
    """
    ip = get_ipython()
    _setup_fugue_notebook(ip, notebook_setup)
    return Javascript(_HIGHLIGHT_JS)
