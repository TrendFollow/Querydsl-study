package study.querydsl.repository;

import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;
import study.querydsl.entity.Member;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.select;
import static study.querydsl.entity.QMember.member;

@Repository
public class MemberTestRepository extends QuerydslRepositorySupport {

    public MemberTestRepository(){
        super(Member.class);
    }


}
