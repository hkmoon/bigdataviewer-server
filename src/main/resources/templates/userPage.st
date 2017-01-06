<!DOCTYPE html>
<html lang='en'>
<head>
    <link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-tagsinput/0.8.0/bootstrap-tagsinput.css"
          rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/x-editable/1.5.1/bootstrap3-editable/css/bootstrap-editable.css"
          rel="stylesheet">
    <style>
        .bootstrap-tagsinput {
            min-width: 100%;
        }
    </style>
</head>
<body>
<div class='container'>
    <h2>BigDataServer DataSet list - $userId$</h2>

    <table class='table table-hover table-bordered'>
        <thead>
        <tr>
            <th>DataSet</th>
            <th>Description</th>
            <th>Share</th>
        </tr>
        </thead>
        <tbody>
        $dataSetTr$
        </tbody>
    </table>

    <div class="well">
        <h4>Add New Dataset</h4>

        <form action="" method="POST">
            <div class="input-group">
                <span class="input-group-addon" id="basic-addon1">Dataset</span>
                <input name="name-text" id="name-text" type="text" class="form-control" placeholder="Name"
                       aria-describedby="basic-addon1"/>

                <input name="tags-text" id="tags-text" type="text" class="form-control"
                       data-role="tagsinput" placeholder="Type a string and ,"
                       aria-describedby="basic-addon1"/>

                <input name="description-text" id="description-text" type="text"
                       class="form-control" placeholder="Description"
                       aria-describedby="basic-addon1"/>
                <input name="file" id="file" type="text" class="form-control" placeholder="Location"
                       aria-describedby="basic-addon1"/>
                <span class="input-group-addon">
                    <label class="form-check-label">
                        <input name="is-public" id="is-public" type="checkbox"
                               class="form-check-input" placeholder="Public"/> Public
                    </label>
                </span>
            </div>

            <button type="button" class="btn btn-info btn-group-justified" id="addDS">
                Add
            </button>
        </form>

        <div id='error' class="alert alert-danger" style="display: none" role="alert">
            <span class="sr-only">Error:</span>
            Error message
        </div>

        <div id='success' class="alert alert-success" style="display: none" role="alert">
            <span class="sr-only">Success:</span>
            Successful message
        </div>
    </div>

</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-tagsinput/0.8.0/bootstrap-tagsinput.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/x-editable/1.5.1/bootstrap3-editable/js/bootstrap-editable.min.js"></script>

$JS$

</body>
</html>