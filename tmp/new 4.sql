	drop table feature_analysis.user_tag_analysis;
	create table feature_analysis.user_tag_analysis stored as orc tblproperties ("orc.compress"="SNAPPY") as
	select userid,collect(tag,weight) weights  from 
		(
			select userid,weight,tag,if(size(split(tag,"-"))==2,split(tag,"-")[0],tag) tag_l1 from 
			(
					select userid,avg(weight) weight,tag  from 
					(
						select t3.userid,t3.wikiid,t3.weight,tag from 
						(
							select 
							t1.userid,
							t1.wikiid,
							t1.weight,
							t2.tags_l1
							from 
							feature_analysis.user_wiki_weight_analysis t1
							join 
							(select * from  base_info.wiki where size(tags_l1)>0 and tags_l1[0]<>'') t2
							on(t1.wikiid=t2.id)
						) t3 lateral view  explode(tags_l1) tagstable as tag
						where length(tag) >0
						union all 
						select userid,wikiid,weight,concat(tags_l1[0],'-',tag)  tag from 
						(
							select t3.userid,t3.wikiid,t3.weight, tag,tags_l1 from 
							(
								select 
								t1.userid,
								t1.wikiid,
								t1.weight/size(tags_l2) weight,
								t2.tags_l2,
								t2.tags_l1
								from 
								feature_analysis.user_wiki_weight_analysis t1
								join 
								(select * from  base_info.wiki where size(tags_l1)>0 and  tags_l1[0]<>'' and size(tags_l2)>0) t2
								on(t1.wikiid=t2.id)
							) t3 lateral view  explode(tags_l2) tagstable as tag
							where length(tag) >0
						)t40
						
					) t4 group by userid,tag
			) t5
	)t6 group by t6.userid,t6.tag_l1;