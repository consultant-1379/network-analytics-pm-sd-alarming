
isEdit = $("#isEdit").text().trim()

if(isEdit == "Edit"){
	$( "#saveAfterEditBtn" ).css("display", "");
	$( "#saveAfterCreateBtn" ).css("display", "none");
	$('#alarm-name').css({
		'pointer-events' : 'none',
		'color' : '#999',
    });
	$('#alarmtype').css({
		'pointer-events' : 'none',
		'color' : '#999',
    });
	$('#system-area').css({
		'pointer-events' : 'none',
		'color' : '#999',
    });
	$('#node-type').css({
		'pointer-events' : 'none',
		'color' : '#999',
    });
	$("isEdit :first-child").css({
		'background' : '#ccc',
	});
}else if(isEdit == "Create"){
	$( "#saveAfterEditBtn" ).css("display", "none");
	$( "#saveAfterCreateBtn" ).css("display", "");
}
	

