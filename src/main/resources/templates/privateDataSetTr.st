<tr>
    <td>
        <img src='$thumbnailUrl$'/>
    </td>
    <td>
        <dl class="row">
            <dt class="col-sm-2">
                Tags
            </dt>
            <dd class="col-sm-9">
                <input name="tagsinput" type="text" value="$dataSetTags$" data-role="tagsinput" dataSetId="$dataSetId$"
                       class="tags-input"/>
            </dd>

            <dt class="col-sm-2">
                Name
            </dt>
            <dd class="col-sm-9">
                <a href="#" id="dataSetName" class="dataSetName" data-type="text" data-pk="$dataSetId$"
                   data-url="?op=updateDS" data-title="Enter DataSet Name">$dataSetName$</a>
            </dd>

            <dt class="col-sm-2">
                Description
            </dt>
            <dd class="col-sm-9">
                <a href="#" id="dataSetDescription" class="dataSetDescription" data-type="text"
                   data-pk="$dataSetId$" data-url="?op=updateDS" data-title="Enter DataSet Description">$dataSetDescription$</a>
            </dd>

            <dt class="col-sm-2">
                Location
            </dt>
            <dd class="col-sm-9">
                <small class="text-muted">$dataSetLocation$</small>
            </dd>

            <dt class="col-sm-2">
                Links
            </dt>
            <dd class="col-sm-9">
                <a href="$dataSetUrl$.xml">XML</a>&nbsp; <a href="$dataSetUrl$.bdv">BDV</a>
            </dd>

            <dt class="col-sm-2">

            </dt>
            <dd class="col-sm-9">
                <button type="button" dataSetId="$dataSetId$" class="btn btn-danger btn-group-justified removeDS">
                    Remove
                </button>
            </dd>
        </dl>
    </td>
    <td>
        <input type="checkbox" class="public-checkbox" dataSetId="$dataSetId$" value="$dataSetIsPublic$"/> <b>public</b>
        <br/><br/>

        $sharedUsers$

        <br/>
        <form action="" dataSetId="$dataSetId$" class="add-new-shared-user">
            <input type="submit" value="Add user"/><br/>
            <input name="userId" type="text" value=""/>
        </form>
    </td>
</tr>