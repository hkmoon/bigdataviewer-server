<script>
    $(document).ready(function () {

        $.fn.editable.defaults.mode = 'inline';
        $.fn.editable.defaults.ajaxOptions = {type: "GET"};

        $('.dataSetName').editable();
        $('.dataSetDescription').editable();

        $('.public-checkbox').each(function () {
            this.checked = $(this).val() == "true";
        });

        $('.tags-input').on('beforeItemAdd', function (event) {
            var tag = event.item;
//            console.info(event.target);

            var ajaxData = {
                data: {
                    dataset: event.target.getAttribute('dataSetId'),
                    tag: event.item,
                    op: 'addTag'
                }
            };

            if (!event.options || !event.options.preventPost) {
                $.ajax('', ajaxData)
                    .fail(function () {
                        // Remove the tag since there was a failure
                        // "preventPost" here will stop this ajax call from running when the tag is removed
//                        $('.tags-input').tagsinput('remove', tag, {preventPost: true});
                    });
            }
        });

        $('.tags-input').on('beforeItemRemove', function (event) {
            var tag = event.item;
            //console.info(event.target);
            var ajaxData = {
                data: {
                    dataset: event.target.getAttribute('dataSetId'),
                    tag: event.item,
                    op: 'removeTag'
                }
            };

            if (!event.options || !event.options.preventPost) {
                $.ajax('', ajaxData)
                    .fail(function () {
                        // Re-add the tag since there was a failure
                        // "preventPost" here will stop this ajax call from running when the tag is added
//                        $('.tags-input').tagsinput('add', tag, {preventPost: true});
                    });
            }
        });

        $('.public-checkbox').click(function () {
            var ajaxData = {
                data: {
                    dataset: this.getAttribute('dataSetId'),
                    checked: this.checked,
                    op: 'setPublic'
                }
            };

            $.ajax('', ajaxData)
                .done(function () {
//                    location.reload(true);
                });
        });

        $('.add-new-shared-user').submit(function (event) {
            event.preventDefault();
            var ajaxData = {
                data: {
                    dataset: event.target.getAttribute('dataSetId'),
                    userId: event.target['userId'].value,
                    op: 'addSharedUser'
                }
            };

            $.ajax('', ajaxData)
                .done(function () {
                    location.reload(true);
                });
        });

        $('.remove-shared-user').click(function () {
            var ajaxData = {
                data: {
                    dataset: this.getAttribute('dataSetId'),
                    userId: this.getAttribute('userId'),
                    op: 'removeSharedUser'
                }
            };

            $.ajax('', ajaxData)
                .done(function () {
                    location.reload(true);
                });
        });

        $('#addDS').on('click', function () {
            $.ajax({
                type: "POST",
                url: "?op=addDS",
                data: {
                    name: $('#name-text').val(),
                    tags: $('#tags-text').val(),
                    description: $('#description-text').val(),
                    file: $('#file').val(),
                    public: $('#is-public').prop('checked')
                },
                success: function (msg) {
                    if (msg.match("^Error")) {
                        $('#error').html(msg);
                        $('#error').fadeIn(600).delay(5000).fadeOut(600);
                    }
                    else if (msg.match("^Success")) {
                        $('.form-control').val("");
                        $('#success').html(msg);
                        $('#success').fadeIn(600).delay(5000).fadeOut(600);
                        location.reload(true);
                    }

                    console.info(msg);
                }
            });
        });

        $('.removeDS').on('click', function () {
            $.ajax({
                type: "POST",
                url: "?op=removeDS",
                data: {dataset: this.getAttribute('dataSetId')},
                success: function (msg) {
                    if (msg.match("^Error")) {
                        $('#error').html(msg);
                        $('#error').fadeIn(600).delay(5000).fadeOut(600);
                    }
                    else if (msg.match("^Success")) {
                        $('.form-control').val("");
                        $('#success').html(msg);
                        $('#success').fadeIn(600).delay(5000).fadeOut(600);
                        location.reload(true);
                    }

                    console.info(msg);
                }
            });
        });
    });

</script>

