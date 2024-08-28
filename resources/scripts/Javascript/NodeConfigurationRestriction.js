var NodeConfiguration = $("#NodeConfiguration").text().trim()

if (NodeConfiguration == "EDIT"){
	$('.NodeConfiguration').css({
	'pointer-events' : 'none',
	'color' : '#999',
    });
	$("NodeConfiguration :first-child").css({
	'background' : '#ccc',
	});
}
	
