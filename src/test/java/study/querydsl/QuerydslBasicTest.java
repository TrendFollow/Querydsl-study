package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() throws Exception {
        // given
        jpaQueryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() throws Exception{
        // given
        String qlString =
                "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        // when
        assertThat(findMember.getUsername()).isEqualTo("member1");

        // then
    }

    @Test
    public void startQuerydsl() throws Exception{
        // given
        
//        new QMember("m") 이 from Member m 으로 Alias 타입이 된다. QMember.member로 하면 기본이 member1 이다
//        QMember m = new QMember("m");
//        QMember m1 = QMember.member;

        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() throws Exception{
        // given
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1").and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() throws Exception{
        // given
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"), (member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() throws Exception{
//        List<Member> fetch = jpaQueryFactory
//                .selectFrom(member)
//                .fetch();
//
//        Member fetchOne = jpaQueryFactory
//                .selectFrom(member)
//                .fetchOne();
//
//        Member fetchFirst = jpaQueryFactory
//                .selectFrom(member)
//                .fetchFirst();

//        QueryResults<Member> result = jpaQueryFactory
//                .selectFrom(member)
//                .fetchResults();

//        result.getTotal();
//        List<Member> content = result.getResults();
//        result.getLimit();
//        result.getOffset();

        long total = jpaQueryFactory
                .selectFrom(member)
                .fetchCount();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 오름차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     *
     */
    @Test
    public void sort() throws Exception{
        // given
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));


        // when
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

        // then
    }

    @Test
    public void paging1() throws Exception{
        // given
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void aggregation() throws Exception{
        List<Tuple> result = jpaQueryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     *
     * 팀의 이름과 각 팀의 평균 연령을 구해라
     */
    @Test
    public void group() throws Exception{
        // given
        List<Tuple> result = jpaQueryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);


        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     *
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join() throws Exception{
        // when
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        // then
        assertThat(result)
                .extracting("username")
                .containsExactly("member1","member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() throws Exception{
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when
        List<Member> fetch = jpaQueryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(fetch)
                .extracting("username")
                .containsExactly("teamA","teamB");
        // then
    }

    /**
     *
     * 회원과 팀 조인, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering() throws Exception{
        List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름과 팀 이름이 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation() throws Exception{
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // when
        // member.team 으로 조인하는 것은 연관관계가 있는 조인
        // 그냥 team으로 조인하는 것은 연관관계가 없는 조인
        List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
        // then
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() throws Exception{
        // given
        em.flush();
        em.clear();

        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();


        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("패치 조인 미적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() throws Exception{
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)))
                .fetch();

        // then
        assertThat(result)
                .extracting("age")
                .containsExactly(40);

    }

    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    public void subQueryGoe() throws Exception{
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)))
                .fetch();

        // then
        assertThat(result)
                .extracting("age")
                .containsExactly(30,40);

    }

    @Test
    public void subQueryIn() throws Exception{
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))))

                .fetch();

        // then
        assertThat(result)
                .extracting("age")
                .containsExactly(20,30,40);

    }

    @Test
    public void selectSubQuery() throws Exception{
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Tuple> result = jpaQueryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
        // then
    }

    @Test
    public void basicCase() throws Exception{
        // given
        List<String> fetch = jpaQueryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : fetch) {
            System.out.println(s);
        }
    }

    @Test
    public void complexCase() throws Exception{
        // given
        List<String> fetch = jpaQueryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : fetch) {
            System.out.println(s);
        }
    }

    @Test
    public void constant() throws Exception{
        List<Tuple> result = jpaQueryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    @Test
    public void concat() throws Exception{
        List<String> result = jpaQueryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    public void simpleProjection() throws Exception{
        List<Member> result = jpaQueryFactory
                .select(member)
                .from(member)
                .fetch();

        for (Member s : result) {
            System.out.println(s);
        }
    }

    @Test
    public void tupleProjection() throws Exception{
        // given
        List<Tuple> result = jpaQueryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    public void findDtoByJPQL() throws Exception{
        // given
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void findDtoBySetter() throws Exception{

        // 반환 타입이 Tuple로 받지 않아도 되고, 원하는 값만 딱 조회하기에 성능에도 좋다!!!
        // setter가 없으면 값은 null 또는 0이다.
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }

    }

    @Test
    public void findDtoByField() throws Exception{

        // 필드에 바로 주입된다.
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }

    }

    @Test
    public void findDtoByConstructor() throws Exception{

        // 생성자로 주입된다. 파라미터 순서가 맞아야한다.
        // 오류 발생 시 컴파일 오류는 잡지 못하고 런타임에 오류가 생긴다.
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }

    }

    @Test
    public void findDtoByConstructor2() throws Exception{

        // 생성자로 주입된다. 파라미터 순서가 맞아야한다.
        List<UserDto> result = jpaQueryFactory
                .select(Projections.constructor(UserDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (UserDto memberDto : result) {
            System.out.println(memberDto);
        }

    }

    @Test
    public void findUserDto() throws Exception{
        QMember memberSub = new QMember("memberSub");

        // 필드에 바로 주입된다.
        List<UserDto> result = jpaQueryFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println(userDto);
        }
    }

    @Test
    public void findDtoByQueryProjection() throws Exception{
        // given
        // 생성자에 @QueryProjection 로 생성자를 Qdto로 만들어주고 컴파일 시점에 파라미터 타입 체크 가능!!!!!
        List<MemberDto> result = jpaQueryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        // when
        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception{
        // given
        String usernameParam = "member1";
        Integer ageParam = 10;

        // when
        List<Member> result = searchMember1(usernameParam, ageParam);

        // then
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameParam, Integer ageParam) {
        BooleanBuilder builder = new BooleanBuilder();

        if(usernameParam != null){
            builder.and(member.username.eq(usernameParam));
        }

        if(ageParam != null){
            builder.and(member.age.eq(ageParam));
        }

        return jpaQueryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }


    @Test
    public void dynamicQuery_WhereParam() throws Exception{
        // given
        String usernameParam = "member1";
        Integer ageParam = 10;

        // when
        List<Member> result = searchMember2(usernameParam, ageParam);

        // then
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameParam, Integer ageParam) {
        return jpaQueryFactory
                .selectFrom(member)
//                where에 null은 무시가 된다.
                .where(usernameEq(usernameParam), ageEq(ageParam))
//                .where(allEq(usernameParam, ageParam))
                .fetch();
    }

    private BooleanExpression ageEq(Integer ageParam) {
        return (ageParam == null) ? null : member.age.eq(ageParam);
    }

    private BooleanExpression usernameEq(String usernameParam) {
        return (usernameParam == null) ? null : member.username.eq(usernameParam);
    }

    private BooleanExpression allEq(String usernameParam, Integer ageParam){
        return usernameEq(usernameParam).and(ageEq(ageParam));
    }

    @Test
    public void bulkUpdate() throws Exception{
        // given
        long count = jpaQueryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        em.clear();
    }

    @Test
    public void bulkAdd() throws Exception{
        // given
        long count = jpaQueryFactory
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();

        em.clear();

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : result) {
            System.out.println(member1);
        }
    }

    @Test
    public void bulkDelete() throws Exception{
        // given
        jpaQueryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    public void sqlFunction() throws Exception{
        List<String> result = jpaQueryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"
                ))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    public void sqlFunction2() throws Exception{
        List<String> result = jpaQueryFactory
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", member.username)))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }
}