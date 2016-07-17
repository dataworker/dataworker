(function($, undefined){
  "use strict";

  $(document).on("click", "#side-nav a:not([data-toggle])", function() {
    $("#side-nav").trigger("click.dismiss.bs.menu");
  });

  $(document).on("shown.bs.menu", function() {
    $("body").scrollspy("refresh");
  });
})(jQuery);
