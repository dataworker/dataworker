(function($, undefined){
  "use strict";

  $(document).on("click", "#side-nav a:not([data-toggle])", function() {
    $("#side-nav").trigger("click.dismiss.bs.menu");
  });

  $(document).on("shown.bs.menu", function() {
    $("body").scrollspy("refresh");
  });

  $(document).on("show.bs.collapse", "#side-nav .collapse", function() {
    $("#side-nav .collapse.in").collapse("hide");
  });
})(jQuery);
