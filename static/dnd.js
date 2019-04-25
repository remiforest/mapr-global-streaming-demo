// Plugin call for drag and drop
$(function(){
    $('.drag').draggable({revert:true,revertDuration: 0});
});



// AJAX call to deploy a new country
$('.drop').droppable({
  drop : function(e){
        var zone = $(this).attr('id');
        $.ajax({
            url: 'deploy_new_country',
            type: 'post',
            data: {"country":zone},
            success:function(data){
                console.log(zone);
                $("#"+zone+"_docker_image").show();
            }
        });
    }
});


// AJAX call to remove a deployed country
$('#recycle').droppable({
  drop : function(e,ui){
        var zone = ui.draggable.attr('id').split("_")[0];
        $("#"+zone+"_docker_image").hide();
        $.ajax({
              url: 'remove_country',
              type: 'post',
              data: {"country":zone},
              success:function(data){
                console.log(zone + " container stopped")
                $("#"+zone+"_chart").remove();
              }
          });
    }
});



// AJAX call to get the list of active streams and display the stream images if a stream exists
active_streams = []
function display_streams(){
    $.ajax({
        url: 'get_source_streams',
        type: 'post',
        success:function(data){
            var current_streams = JSON.parse(data);
            $.each(current_streams,function(index,value){
                $("#" + value + "_streams").show();
                $("#" + value + "_docker_image").show();
            });
            $.each(active_streams,function(index,value){
                if (!current_streams.includes(value)){
                    $("#" + value + "_streams").hide();
                    $("#" + value + "_docker_image").hide();
                }
            });
            active_streams = current_streams;
            if (active_streams.length>0){
              $("#replicate_streams").show();
            }else{
              $("#replicate_streams").hide();
              $("#global_streams").hide();
            }
            setTimeout(function(){
                display_streams();
              }, 1000);
        }
    });
}



$("#replicate_streams").click(function(){
    $("#replicate_streams").text("Replicating ...");
    $.ajax({
        url: 'replicate_streams',
        type: 'get',
        success:function(data){
            setTimeout(function(){
              $("#replicate_streams").text("Replicate streams");
              $("#global_streams").show();
            }, 1000 * 10);
          }
    });
});




function create_chart(country){
    console.log("creating chart for " + country);
    if (!$('#'+ country + "_chart").length){
        $("#" + country + "_charts").append("<div class='dnd_charts' id='"+country+"_chart'></div>");
    }
    var chart_div = country+"_chart";
    var myChart = Highcharts.chart(chart_div, {
        chart: {
            type: 'column',
            height : 200,
            width : 300,
/*            backgroundColor:'rgba(255, 255, 255, 0.0)'
*/        },
        title: {
            text: country
        },
        xAxis: {
            categories: [],
            labels:
              {
                enabled: false
              }
        },
        yAxis: {
            title: {
                text: 'Count'
            }
        },
        series: [{
            showInLegend: false, 
            data: []
        }]
    });
    return myChart;

}


function update_country_charts(){
    console.log("update data for each chart");
    $.ajax({
        url: 'get_country_stream_data',
        type: 'post',
        success:function(data){
            console.log(data);
            loaded_data = JSON.parse(data);
            // Iterate over object
            $.each(loaded_data,function(key,value){
                // get or create chart
                if (!$('#' + key + "_chart").length){
                    create_chart(key);
                }
                var chart=$("#" + key + "_chart").highcharts();
                // get and update values
                var chart_data = chart.series[0].data;
                var new_data = [];
                var categories = [];
                for (var i = 0; i < chart_data.length; i++) {
                    category = chart_data[i].category;
                    categories.push(category);
                    current_value = chart_data[i].y;
                    incoming_value = value[category];
                    if (incoming_value){
                        new_value = current_value + incoming_value;
                    }else{
                        new_value = current_value;
                    }
                    new_data.push(new_value);
                    delete value[category];
                }
                for (var category in value){
                    if (category != "count"){
                        categories.push(category);
                        new_data.push(value[category]);                        
                    }
                }
                chart.xAxis[0].setCategories(categories);
                chart.series[0].setData(new_data);  
            })
            
            setTimeout(function(){
                update_country_charts();
              }, 1000 * 2);
        }
    });
}


$( ".show_chart" ).mouseover(function() {
  var country = $(this).attr("id").split('_')[0];
  $("#"+country+"_charts").show();
});

$( ".show_chart" ).mouseout(function() {
  var country = $(this).attr("id").split('_')[0];
  $("#"+country+"_charts").hide();
});


$( document ).ready(function(){
  display_streams();
  update_country_charts();
});
