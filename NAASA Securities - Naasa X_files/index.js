var ticksidebar = 0;
var mnav = 0;
var heading = $('#heading_prod5').html();
var desc = $('#desc_prod5').html();

var headingCalc = $('#hcalc').html();
var descCalc = $('#desc_calc').html();
$(document).ready(function () {

    // Market Exchange
    $(".EC-btn").click(function () {
        if (ticksidebar == 0) {
            $(this).addClass("active");
            $(".hide-Minfo").slideUp();
            $(".small-exch").slideDown();
            $(".exch-sto-price").addClass("active");
            ticksidebar = 1;
        } else {
            $(this).removeClass("active");
            $(".hide-Minfo").slideDown();
            $(".small-exch").slideUp();
            $(".exch-sto-price").removeClass("active");
            ticksidebar = 0;
        }
    });

    // Main Menu
    $('.link').mouseover(function () {
        $('#hcalc').html($(this).data("name"));
        $('#desc_calc').html($(this).data("description"));
    });
    $('.link').mouseout(function () {
        $('#hcalc').html(headingCalc);
        $('#desc_calc').html(descCalc);
    });


    $('.link').mouseover(function () {
        $('#heading_prod5').html($(this).data("name"));
        $('#desc_prod5').html($(this).data("description"));
    });
    $('.link').mouseout(function () {
        $('#heading_prod5').html(heading);
        $('#desc_prod5').html(desc);
    });

    $("#mobnav").click(function () {
        $(".blaze-menu > ul").slideToggle();
        $("#mobnav").toggleClass('active');
    });

    $('.cansubnav').hover(function () {
        if (mnav == 0) {
            $(".bgfade").show();
            mnav = 1;
        } else {
            $(".bgfade").hide();
            mnav = 0;
        }
    });

    // Nice Select
    $('select:not(.ignore)').niceSelect();
});

//Doughnut chart
//$('#margin-chart').doughnutChart({
//    positiveColor: "#DA4453",
//    negativeColor: "#3BB0D6",
//    backgroundColor: "white",
//    percentage: 52,
//    size: 100,
//    doughnutSize: 0.35,
//    innerText: "52%",
//    innerTextOffset: 12,
//    Title: "Margin",
//    positiveText: "52.5% Barack Obama",
//    negativeText: "47.5% Others"
//});

//Scheme popup
$('.popup-with-move-anim').magnificPopup({
    type: 'inline',
    fixedContentPos: false,
    fixedBgPos: true,
    overflowY: 'auto',
    closeBtnInside: true,
    preloader: false,
    midClick: true,
    removalDelay: 300,
    mainClass: 'my-mfp-slide-bottom'
});
