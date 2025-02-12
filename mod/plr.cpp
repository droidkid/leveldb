#include "plr.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include<cmath>

using std::string;


// Code modified from https://github.com/RyanMarcus/plr

double
get_slope(struct point p1, struct point p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
}

struct line
get_line(struct point p1, struct point p2) {
    double a = get_slope(p1, p2);
    double b = -a * p1.x + p1.y;
    struct line l{.a = a, .b = b};
    return l;
}

struct point
get_intersetction(struct line l1, struct line l2) {
    double a = l1.a;
    double b = l2.a;
    double c = l1.b;
    double d = l2.b;
    struct point p {(d - c) / (a - b), (a * d - b * c) / (a - b)};
    return p;
}

bool
is_above(struct point pt, struct line l) {
    return pt.y > l.a * pt.x + l.b;
}

bool
is_below(struct point pt, struct line l) {
    return pt.y < l.a * pt.x + l.b;
}

struct point
get_upper_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y + gamma};
    return p;
}

struct point
get_lower_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y - gamma};
    return p;
}

GreedyPLR::GreedyPLR(double gamma) {
    this->state = "need2";
    this->gamma = gamma;
}

int counter = 0;

uint64_t LdbKeyToInteger(const std::string& str) {
    const char* data = str.data();
    size_t size = str.size();
    uint64_t num = 0;
    bool leading_zeros = true;

    for (int i = 0; i < size; ++i) {
        int temp = data[i];
        // TODO: Figure out where the extra bytes are coming from
        if (temp < '0' || temp >'9') break;
        if (leading_zeros && temp == '0') continue;
        leading_zeros = false;
        num = (num << 3) + (num << 1) + temp - 48;
    }
    return num;
}


Segment
GreedyPLR::process(const struct point& pt) {
    Segment s = {0, 0, 0};
    if (this->state.compare("need2") == 0) {
        this->s0 = pt;
        this->state = "need1";
    } else if (this->state.compare("need1") == 0) {
        this->s1 = pt;
        setup();
        this->state = "ready";
    } else if (this->state.compare("ready") == 0) {
        s = process__(pt);
    } else {
        // impossible
        std::cout << "ERROR in process" << std::endl;
    }
    this->last_pt = pt;
    return s;
}

void
GreedyPLR::setup() {
    this->rho_lower = get_line(get_upper_bound(this->s0, this->gamma),
                               get_lower_bound(this->s1, this->gamma));
    this->rho_upper = get_line(get_lower_bound(this->s0, this->gamma),
                               get_upper_bound(this->s1, this->gamma));
    this->sint = get_intersetction(this->rho_upper, this->rho_lower);
}

Segment
GreedyPLR::current_segment() {
    uint64_t segment_start = this->s0.x;
    double avg_slope = (this->rho_lower.a + this->rho_upper.a) / 2.0;
    double intercept = -avg_slope * this->sint.x + this->sint.y;
    Segment s = {segment_start, avg_slope, intercept};
    return s;
}

Segment
GreedyPLR::process__(struct point pt) {
    if (!(is_above(pt, this->rho_lower) && is_below(pt, this->rho_upper))) {
        // new point out of error bounds
        Segment prev_segment = current_segment();
        this->s0 = pt;
        this->state = "need1";
        return prev_segment;
    }

    struct point s_upper = get_upper_bound(pt, this->gamma);
    struct point s_lower = get_lower_bound(pt, this->gamma);
    if (is_below(s_upper, this->rho_upper)) {
        this->rho_upper = get_line(this->sint, s_upper);
    }
    if (is_above(s_lower, this->rho_lower)) {
        this->rho_lower = get_line(this->sint, s_lower);
    }
    Segment s = {0, 0, 0};
    return s;
}

Segment
GreedyPLR::finish() {
    Segment s = {0, 0, 0};
    if (this->state.compare("need2") == 0) {
        this->state = "finished";
        return s;
    } else if (this->state.compare("need1") == 0) {
        this->state = "finished";
        s.x = this->s0.x;
        s.k = 0;
        s.b = this->s0.y;
        return s;
    } else if (this->state.compare("ready") == 0) {
        this->state = "finished";
        return current_segment();
    } else {
        std::cout << "ERROR in finish" << std::endl;
        return s;
    }
}

PLR::PLR(double gamma) {
    this->gamma = gamma;
}

std::vector<Segment>&
PLR::train(std::vector<string>& keys) {
    GreedyPLR plr(this->gamma);
    size_t size = keys.size();
    int num_key = 0;
    uint64_t prev_key = 0;
    for (int i = 0; i < size; ++i) {
        uint64_t current_key = LdbKeyToInteger(keys[i]);
        if (i && prev_key == current_key) {
            continue;
        }
        Segment seg = plr.process(point((double) current_key, i));
        if (seg.x != 0 ||
            seg.k != 0 ||
            seg.b != 0) {
            this->segments.push_back(seg);
        }
        prev_key = current_key;
    }

    Segment last = plr.finish();
    if (last.x != 0 ||
        last.k != 0 ||
        last.b != 0) {
        this->segments.push_back(last);
    }

    return this->segments;
}