$('.tooltip').css({
	'position' : 'relative',
	'display' : 'inline-block',
	'border-bottom' : '1px dotted black'
});

$('.tooltip .tooltiptext').css({
	'visibility' : 'hidden',
	'width' : '120px',
	'background-color' : 'black',
	'color' : '#fff',
	'text-align' : 'center',
	'padding' : '5px 0',
	'border-radius' : '6px',
	'position' : 'absolute',
	'z-index' : '1'
});

$('.tooltip:hover .tooltiptext').css({
	'visibility' : 'visible'
});