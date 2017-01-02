<tr>
    <td>
        <img src='$thumnailUrl$'/>
    </td>
    <td>
        Tags:<input type="text" dataSetId="$dataSetId$" class="tags-input" value="$dataSetTags$" data-role="tagsinput"/><br/>
        Name: <a href="#" id="dataSetName" class="dataSetName" data-type="text" data-pk="$dataSetId$"
                 data-url="?op=updateDS" data-title="Enter DataSet Name">$dataSetName$</a><br/>

        Description: <a href="#" id="dataSetDescription" class="dataSetDescription" data-type="text"
                        data-pk="$dataSetId$" data-url="?op=updateDS" data-title="Enter DataSet Description">$dataSetDescription$</a><br/>
        <a href="$dataSetUrl$.xml">XML</a>&nbsp; <a href="$dataSetUrl$.bdv">BDV</a><br/>
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